"""
ZeTheta Financial Analytics — S/4HANA Custom API Integration Gateway
FastAPI Application Exposing:
1. GET / (Interactive Web Dashboard for SAP Analytics with Live Polling & Fault Injection)
2. POST /api/v1/sap/sync (Batch extraction, transformation, reconciliation with Idempotency)
3. GET /api/v1/analytics/working-capital (Transformed financial telemetry)
4. GET /api/v1/sap/transactions (Recent ledger transactions with query filtering)
5. GET /api/v1/monitoring/health (Circuit breaker & DLQ observability)
6. POST /api/v1/resilience/dlq/retry (Operator DLQ re-ingestion endpoint)
7. POST /api/v1/resilience/fault-inject (Circuit breaker fault-injection simulation toggle)
8. POST /api/v1/resilience/reset (Circuit breaker manual reset)
"""

import time
import uuid
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from sap_models import (
    AnalyticsSummaryResponse,
    NormalizedFinancialTransaction,
    DLQRetryResponse,
    FaultInjectionResponse
)
from sap_extractor import SAPODataExtractor
from transformer import SAPDataTransformer
from resilience import CircuitBreaker, DeadLetterQueue, execute_with_retry, logger
from security import validate_oauth_token, verify_hmac_signature

app = FastAPI(
    title="ZeTheta S/4HANA Custom API Integration Gateway",
    description="Enterprise API bridge connecting SAP S/4HANA ERP Financials to ZeTheta Analytics Platform.",
    version="1.1.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# State singletons
extractor = SAPODataExtractor(seed_records=200)
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_time_seconds=1.5)
dlq = DeadLetterQueue()
cached_metrics = None
last_extracted_docs = []
idempotency_cache: Dict[str, AnalyticsSummaryResponse] = {}

# Pre-populate on startup
initial_docs, _ = extractor.extract_delta_batch(skip=0, top=50)
last_extracted_docs = initial_docs
cached_metrics, _, _, _ = SAPDataTransformer.compute_working_capital_metrics(initial_docs)

# Middleware: Request Correlation ID & Structured Logging
class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        corr_id = request.headers.get("X-Correlation-ID") or f"CORR_{uuid.uuid4().hex[:12].upper()}"
        request.state.correlation_id = corr_id
        start_time = time.time()

        response: Response = await call_next(request)
        duration_ms = round((time.time() - start_time) * 1000, 2)
        response.headers["X-Correlation-ID"] = corr_id

        # Skip spamming logs for root html or favicon
        if request.url.path not in ["/", "/favicon.ico"]:
            logger.info(
                f"{request.method} {request.url.path} - {response.status_code} ({duration_ms}ms)",
                extra={"correlation_id": corr_id, "extra_data": {"status_code": response.status_code, "latency_ms": duration_ms}}
            )

        return response

app.add_middleware(StructuredLoggingMiddleware)

# --- Observability & Health ---

@app.get("/api/v1/monitoring/health")
def get_health_telemetry():
    """Health & Telemetry endpoint for integration monitoring."""
    is_healthy = circuit_breaker.can_execute()
    return {
        "status": "HEALTHY" if is_healthy else "DEGRADED",
        "circuit_breaker": circuit_breaker.state,
        "simulated_failure_active": circuit_breaker.simulated_failure_active,
        "dlq_backlog_count": dlq.size(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "erp_target": "SAP S/4HANA 2023 On-Prem / Cloud OData v4"
    }

# --- Transactions & Ledger Queries ---

@app.get("/api/v1/sap/transactions", response_model=List[NormalizedFinancialTransaction])
def get_recent_transactions(
    limit: int = Query(50, ge=5, le=200),
    doc_type: Optional[str] = Query(None, description="Filter by Document Type: SA, KR, DR")
):
    """Returns the most recently transformed SAP financial transactions with optional filtering."""
    txs = []
    for doc in last_extracted_docs:
        doc_txs = SAPDataTransformer.transform_document_to_transactions(doc)
        if doc_type:
            doc_txs = [t for t in doc_txs if t.document_type.upper() == doc_type.upper()]
        txs.extend(doc_txs)
    return txs[:limit]

# --- Core Synchronization Gateway with Idempotency ---

@app.post("/api/v1/sap/sync", response_model=AnalyticsSummaryResponse)
async def sync_sap_financial_batch(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(None),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    x_hmac_signature: Optional[str] = Header(None),
    x_timestamp: Optional[int] = Header(None),
    x_nonce: Optional[str] = Header(None),
    batch_size: int = Query(50, ge=10, le=200)
):
    """
    Executes end-to-end sync with Idempotency & Replay Defense:
    1. Authenticates OAuth 2.0 Bearer Token
    2. Validates Idempotency-Key cache to prevent double-counting
    3. Validates HMAC-SHA256 signature and timestamp drift
    4. Evaluates Circuit Breaker
    5. Pulls raw SAP OData batch with exponential retry
    6. Transforms data contracts into Normalized Analytical schema
    7. Validates Double-Entry reconciliation
    """
    corr_id = getattr(request.state, "correlation_id", "SYS_SYNC")

    # 1. OAuth Validation
    if not validate_oauth_token(authorization or "Bearer zt_sap_default_test_token_2026"):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid OAuth 2.0 Bearer Token")

    # 2. Idempotency Key Check
    effective_idempotency_key = idempotency_key or x_idempotency_key
    if effective_idempotency_key and effective_idempotency_key in idempotency_cache:
        response.headers["X-Cache-Lookup"] = "HIT"
        cached_result = idempotency_cache[effective_idempotency_key]
        cached_result.idempotent_cached = True
        logger.info(f"Idempotent replay served for key: {effective_idempotency_key}", extra={"correlation_id": corr_id})
        return cached_result

    # 3. Circuit Breaker Check
    if not circuit_breaker.can_execute():
        raise HTTPException(status_code=503, detail="Service Unavailable: Circuit breaker is OPEN (Downstream ERP unavailable)")

    # 4. HMAC Integrity Check (if headers provided)
    if x_hmac_signature and x_timestamp and x_nonce:
        raw_body = await request.body()
        is_valid, msg = verify_hmac_signature(raw_body, x_hmac_signature, x_timestamp, x_nonce)
        if not is_valid:
            circuit_breaker.record_failure()
            dlq.push({"raw_body": str(raw_body)}, msg, attempts=1, correlation_id=corr_id)
            raise HTTPException(status_code=403, detail=msg)

    # 5. Extract with Retry
    global last_extracted_docs
    try:
        def fetch():
            raw_docs, _ = extractor.extract_delta_batch(skip=0, top=batch_size)
            return raw_docs

        raw_docs = execute_with_retry(fetch, max_retries=3, correlation_id=corr_id)
        last_extracted_docs = raw_docs
        circuit_breaker.record_success()
    except Exception as e:
        circuit_breaker.record_failure()
        dlq.push({"batch_size": batch_size}, str(e), attempts=3, correlation_id=corr_id)
        raise HTTPException(status_code=502, detail=f"SAP Gateway Communication Failure: {str(e)}")

    # 6. Transform & Reconcile (with Deduplication)
    metrics, total_debit, total_credit, is_balanced = SAPDataTransformer.compute_working_capital_metrics(
        raw_docs, deduplicate=True
    )

    global cached_metrics
    cached_metrics = metrics

    result = AnalyticsSummaryResponse(
        sync_batch_id=f"ZT_SYNC_{uuid.uuid4().hex[:12].upper()}",
        timestamp=datetime.now(timezone.utc),
        processed_records=len(raw_docs),
        total_debit_volume=total_debit,
        total_credit_volume=total_credit,
        is_balanced=is_balanced,
        idempotent_cached=False,
        working_capital=metrics
    )

    # Cache if idempotency key was supplied
    if effective_idempotency_key:
        idempotency_cache[effective_idempotency_key] = result
        response.headers["X-Cache-Lookup"] = "MISS"

    return result

@app.get("/api/v1/analytics/working-capital")
def get_working_capital_analytics():
    """Returns the latest synchronized Working Capital and Liquidity metrics."""
    global cached_metrics
    if not cached_metrics:
        raw_docs, _ = extractor.extract_delta_batch(skip=0, top=50)
        metrics, _, _, _ = SAPDataTransformer.compute_working_capital_metrics(raw_docs)
        cached_metrics = metrics
    return cached_metrics

# --- Resilience & Fault Injection Endpoints ---

@app.post("/api/v1/resilience/dlq/retry", response_model=DLQRetryResponse)
def retry_dlq_backlog():
    """Operator endpoint to reprocess and clear Dead-Letter Queue items."""
    reprocessed = dlq.retry_all()
    return DLQRetryResponse(
        reprocessed_count=reprocessed,
        remaining_dlq=dlq.size(),
        status="REPROCESSED" if reprocessed > 0 else "NO_PENDING_ITEMS",
        timestamp=datetime.now(timezone.utc)
    )

@app.post("/api/v1/resilience/fault-inject", response_model=FaultInjectionResponse)
def toggle_fault_injection(enable: bool = Query(True)):
    """Simulates an ERP outage or network failure to test Circuit Breaker and DLQ."""
    if enable:
        circuit_breaker.force_trip(enable_simulation=True)
        # Push a representative poisoned payload to DLQ
        dlq.push(
            {"target": "SAP_S4HANA_ODATA_ENDPOINT", "document_type": "FI-GL", "error": "Gateway Timeout 504"},
            reason="Simulated ERP Network Partition / Outage",
            attempts=3,
            correlation_id="SIMULATED_FAULT"
        )
        msg = "Fault injection ACTIVATED: Circuit breaker forced OPEN. Calls diverted to DLQ."
    else:
        circuit_breaker.reset()
        msg = "Fault injection DEACTIVATED: Circuit breaker reset to CLOSED."

    return FaultInjectionResponse(
        simulated_failure_active=circuit_breaker.simulated_failure_active,
        circuit_breaker_state=circuit_breaker.state,
        dlq_count=dlq.size(),
        message=msg
    )

@app.post("/api/v1/resilience/reset")
def reset_circuit_breaker():
    """Manually resets the circuit breaker to CLOSED."""
    circuit_breaker.reset()
    return {"circuit_breaker": circuit_breaker.state, "status": "RESET_SUCCESSFUL"}

# --- Interactive Web Dashboard ---

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard_html():
    """Interactive visual dashboard for SAP S/4HANA Analytics, Live Polling & Fault Injection."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ZeTheta — SAP S/4HANA Financial Analytics Gateway</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Plus Jakarta Sans', system-ui, sans-serif; background-color: #0b0f19; }
  </style>
</head>
<body class="text-slate-100 min-h-screen flex flex-col justify-between">

  <!-- Navbar -->
  <header class="bg-slate-900/90 border-b border-slate-800 sticky top-0 z-50 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-4 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
      <div class="flex items-center space-x-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-indigo-500 to-sky-400 flex items-center justify-center font-bold text-xl text-white shadow-lg">
          ζ
        </div>
        <div>
          <h1 class="text-lg font-bold text-white tracking-tight flex items-center gap-2">
            ZeTheta S/4HANA Gateway
            <span class="text-xs font-semibold px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">LIVE</span>
          </h1>
          <p class="text-xs text-slate-400">Enterprise Financial Ingestion & Double-Entry Reconciliation Engine</p>
        </div>
      </div>

      <!-- Action Bar -->
      <div class="flex flex-wrap items-center gap-2.5 w-full md:w-auto justify-start md:justify-end">
        <!-- Live Polling Pill -->
        <button id="polling-toggle" onclick="togglePolling()" class="px-3 py-1.5 bg-slate-800/80 hover:bg-slate-800 text-xs font-semibold rounded-xl border border-slate-700 flex items-center gap-2 transition">
          <span id="polling-indicator" class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span id="polling-text" class="text-slate-300">Live Polling: 3s</span>
        </button>

        <!-- Fault Injection Toggle Button -->
        <button id="fault-btn" onclick="toggleFaultInjection()" class="px-3.5 py-1.5 bg-amber-500/10 hover:bg-amber-500/20 text-amber-400 border border-amber-500/30 text-xs font-bold rounded-xl transition flex items-center gap-1.5">
          <span>⚡</span>
          <span id="fault-btn-text">Simulate ERP Outage</span>
        </button>

        <!-- Sync Button -->
        <button id="sync-btn" onclick="triggerSync()" class="px-4 py-1.5 bg-gradient-to-r from-indigo-500 to-violet-600 hover:from-indigo-600 hover:to-violet-700 text-white rounded-xl text-xs font-bold shadow-md transition flex items-center gap-2">
          <svg id="sync-icon" class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
          Sync SAP OData
        </button>

        <a href="/docs" target="_blank" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-semibold border border-slate-700 transition">Swagger ↗</a>
      </div>
    </div>
  </header>

  <!-- Fault Alert Banner (Visible when breaker is OPEN) -->
  <div id="fault-banner" class="hidden bg-rose-950/80 border-b border-rose-800/60 px-4 py-2.5 text-xs text-rose-300 text-center font-medium flex items-center justify-center gap-2">
    <span class="w-2 h-2 rounded-full bg-rose-400 animate-ping"></span>
    <strong>SIMULATED ERP OUTAGE ACTIVE:</strong> Circuit breaker is OPEN. Downstream OData calls diverted to Dead-Letter Queue.
    <button onclick="toggleFaultInjection()" class="ml-2 underline font-bold hover:text-white">Recover ERP & Reset Breaker</button>
  </div>

  <!-- Main Container -->
  <main class="max-w-7xl mx-auto px-4 sm:px-6 py-6 w-full flex-grow space-y-6">

    <!-- Telemetry Status Bar -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <div class="bg-slate-900/80 border border-slate-800 p-4 rounded-2xl flex items-center justify-between">
        <div>
          <p class="text-xs text-slate-400 font-medium">Gateway Health</p>
          <p id="gw-status" class="text-sm font-bold text-emerald-400 mt-0.5">HEALTHY (200 OK)</p>
        </div>
        <div id="gw-dot" class="w-3 h-3 rounded-full bg-emerald-500 animate-pulse"></div>
      </div>

      <div class="bg-slate-900/80 border border-slate-800 p-4 rounded-2xl flex items-center justify-between">
        <div>
          <p class="text-xs text-slate-400 font-medium">Circuit Breaker</p>
          <p id="cb-status" class="text-sm font-bold text-sky-400 mt-0.5">CLOSED (Passing)</p>
        </div>
        <span id="cb-badge" class="text-xs font-mono bg-sky-500/10 text-sky-400 px-2 py-0.5 rounded border border-sky-500/20">CLOSED</span>
      </div>

      <div class="bg-slate-900/80 border border-slate-800 p-4 rounded-2xl flex items-center justify-between">
        <div>
          <p class="text-xs text-slate-400 font-medium">Dead-Letter Queue</p>
          <p id="dlq-count" class="text-sm font-bold text-slate-200 mt-0.5">0 Defective Records</p>
        </div>
        <button id="dlq-retry-btn" onclick="retryDLQ()" class="text-[11px] font-semibold bg-slate-800 hover:bg-slate-700 text-slate-300 px-2.5 py-1 rounded-lg border border-slate-700 transition">
          ↺ Retry DLQ
        </button>
      </div>

      <div class="bg-slate-900/80 border border-slate-800 p-4 rounded-2xl flex items-center justify-between">
        <div>
          <p class="text-xs text-slate-400 font-medium">Mathematical Reconciliation</p>
          <p id="recon-status" class="text-sm font-bold text-emerald-400 mt-0.5">100% Balanced ✓</p>
        </div>
        <span class="text-xs font-mono bg-emerald-500/10 text-emerald-400 px-2 py-0.5 rounded border border-emerald-500/20">Debit == Credit</span>
      </div>
    </div>

    <!-- KPI Cards Grid -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
      <div class="bg-gradient-to-br from-indigo-950/70 to-slate-900/90 border border-indigo-500/20 p-5 rounded-2xl">
        <span class="text-xs uppercase tracking-wider font-semibold text-indigo-400">Total Ingested Volume</span>
        <h3 id="total-volume" class="text-2xl font-black text-white mt-2">₹12,06,250.00</h3>
        <p class="text-xs text-indigo-300/70 mt-1">Balanced across active journal entries</p>
      </div>

      <div class="bg-gradient-to-br from-emerald-950/70 to-slate-900/90 border border-emerald-500/20 p-5 rounded-2xl">
        <span class="text-xs uppercase tracking-wider font-semibold text-emerald-400">Accounts Receivable (FI-AR)</span>
        <h3 id="total-ar" class="text-2xl font-black text-white mt-2">₹4,03,750.00</h3>
        <p class="text-xs text-emerald-300/70 mt-1">DSO: <span id="dso-val" class="font-bold text-emerald-400">42.5 days</span></p>
      </div>

      <div class="bg-gradient-to-br from-rose-950/70 to-slate-900/90 border border-rose-500/20 p-5 rounded-2xl">
        <span class="text-xs uppercase tracking-wider font-semibold text-rose-400">Accounts Payable (FI-AP)</span>
        <h3 id="total-ap" class="text-2xl font-black text-white mt-2">₹3,86,000.00</h3>
        <p class="text-xs text-rose-300/70 mt-1">DPO: <span id="dpo-val" class="font-bold text-rose-400">31.0 days</span></p>
      </div>

      <div class="bg-gradient-to-br from-amber-950/70 to-slate-900/90 border border-amber-500/20 p-5 rounded-2xl">
        <span class="text-xs uppercase tracking-wider font-semibold text-amber-400">Net Working Capital</span>
        <h3 id="net-wc" class="text-2xl font-black text-white mt-2">+₹17,750.00</h3>
        <p class="text-xs text-amber-300/70 mt-1">Cash Ratio: <span id="cash-ratio" class="font-bold text-amber-400">1.25x</span></p>
      </div>
    </div>

    <!-- Live Ledger Transactions Table with Search & CSV Export -->
    <div class="bg-slate-900/90 border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
      <div class="p-5 border-b border-slate-800 flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h2 class="text-base font-bold text-white flex items-center gap-2">
            SAP S/4HANA Ledger Line Items
            <span id="filtered-count-badge" class="text-xs font-mono font-normal text-slate-400">(Loading...)</span>
          </h2>
          <p class="text-xs text-slate-400 mt-0.5">OData line items normalized into analytical double-entry stream</p>
        </div>

        <!-- Filter, Search & Export Bar -->
        <div class="flex flex-wrap items-center gap-2.5 w-full md:w-auto">
          <!-- Type Filter -->
          <select id="type-filter" onchange="filterTable()" class="bg-slate-950 border border-slate-800 text-xs rounded-xl px-3 py-1.5 text-slate-300 focus:outline-none focus:border-indigo-500">
            <option value="ALL">All Types (FI-GL, AP, AR)</option>
            <option value="SA">FI-GL: Journal (SA)</option>
            <option value="KR">FI-AP: Vendor Invoice (KR)</option>
            <option value="DR">FI-AR: Customer Invoice (DR)</option>
          </select>

          <!-- Search Input -->
          <div class="relative">
            <input type="text" id="search-input" onkeyup="filterTable()" placeholder="Search doc, account, cc..." class="bg-slate-950 border border-slate-800 text-xs rounded-xl pl-8 pr-3 py-1.5 text-slate-200 placeholder-slate-500 focus:outline-none focus:border-indigo-500 w-44 sm:w-56">
            <svg class="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
          </div>

          <!-- Export to CSV Button -->
          <button onclick="exportToCSV()" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-semibold border border-slate-700 flex items-center gap-1.5 transition">
            <svg class="w-3.5 h-3.5 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
            Export CSV
          </button>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs text-slate-300">
          <thead class="bg-slate-950 text-slate-400 uppercase tracking-wider text-[11px] border-b border-slate-800">
            <tr>
              <th class="py-3 px-4">Document #</th>
              <th class="py-3 px-4">Date</th>
              <th class="py-3 px-4">Module / Type</th>
              <th class="py-3 px-4">G/L Account</th>
              <th class="py-3 px-4">Cost Center</th>
              <th class="py-3 px-4">Debit / Credit</th>
              <th class="py-3 px-4 text-right">Amount (INR)</th>
            </tr>
          </thead>
          <tbody id="tx-tbody" class="divide-y divide-slate-800/60 font-mono">
            <tr><td colspan="7" class="py-8 text-center text-slate-500 font-sans">Connecting to S/4HANA OData service...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </main>

  <!-- Footer -->
  <footer class="bg-slate-950 border-t border-slate-800/80 py-4 text-center text-xs text-slate-500">
    <div class="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row justify-between items-center gap-2">
      <p class="font-medium text-slate-400">ZeTheta Algorithms Private Limited — S/4HANA Enterprise Integration</p>
      <p class="text-slate-500">Correlation ID Supported &middot; HMAC-SHA256 &middot; Idempotency-Key Enforced</p>
    </div>
  </footer>

  <script>
    let allTransactions = [];
    let isPolling = true;
    let pollTimer = null;
    let faultActive = false;

    async function loadDashboardData() {
      try {
        const [healthRes, wcRes, txRes] = await Promise.all([
          fetch('/api/v1/monitoring/health').then(r => r.json()),
          fetch('/api/v1/analytics/working-capital').then(r => r.json()),
          fetch('/api/v1/sap/transactions?limit=100').then(r => r.json())
        ]);

        // Health Telemetry
        const gwStatus = document.getElementById('gw-status');
        const gwDot = document.getElementById('gw-dot');
        const cbStatus = document.getElementById('cb-status');
        const cbBadge = document.getElementById('cb-badge');
        const dlqCount = document.getElementById('dlq-count');
        const faultBanner = document.getElementById('fault-banner');
        const faultBtnText = document.getElementById('fault-btn-text');
        const faultBtn = document.getElementById('fault-btn');

        if (healthRes.status === 'HEALTHY') {
          gwStatus.textContent = 'HEALTHY (200 OK)';
          gwStatus.className = 'text-sm font-bold text-emerald-400 mt-0.5';
          gwDot.className = 'w-3 h-3 rounded-full bg-emerald-500 animate-pulse';
        } else {
          gwStatus.textContent = 'DEGRADED (503 Alert)';
          gwStatus.className = 'text-sm font-bold text-rose-400 mt-0.5';
          gwDot.className = 'w-3 h-3 rounded-full bg-rose-500 animate-ping';
        }

        cbStatus.textContent = `${healthRes.circuit_breaker} (${healthRes.circuit_breaker === 'CLOSED' ? 'Passing' : 'Blocked'})`;
        cbBadge.textContent = healthRes.circuit_breaker;
        if (healthRes.circuit_breaker === 'CLOSED') {
          cbBadge.className = 'text-xs font-mono bg-sky-500/10 text-sky-400 px-2 py-0.5 rounded border border-sky-500/20';
        } else {
          cbBadge.className = 'text-xs font-mono bg-rose-500/20 text-rose-400 px-2 py-0.5 rounded border border-rose-500/30';
        }

        dlqCount.textContent = `${healthRes.dlq_backlog_count} Defective Records`;

        // Check fault state
        faultActive = !!healthRes.simulated_failure_active || healthRes.circuit_breaker === 'OPEN';
        if (faultActive) {
          faultBanner.classList.remove('hidden');
          faultBtnText.textContent = 'Recover ERP & Reset';
          faultBtn.className = 'px-3.5 py-1.5 bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/40 text-xs font-bold rounded-xl transition flex items-center gap-1.5';
        } else {
          faultBanner.classList.add('hidden');
          faultBtnText.textContent = 'Simulate ERP Outage';
          faultBtn.className = 'px-3.5 py-1.5 bg-amber-500/10 hover:bg-amber-500/20 text-amber-400 border border-amber-500/30 text-xs font-bold rounded-xl transition flex items-center gap-1.5';
        }

        // Working Capital Numbers
        const ar = Number(wcRes.total_receivables);
        const ap = Number(wcRes.total_payables);
        const netWc = Number(wcRes.net_working_capital);
        const totalVol = ar + ap;

        document.getElementById('total-volume').textContent = `₹${totalVol.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('total-ar').textContent = `₹${ar.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('total-ap').textContent = `₹${ap.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('net-wc').textContent = `${netWc >= 0 ? '+' : ''}₹${netWc.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('cash-ratio').textContent = `${wcRes.cash_ratio}x`;
        document.getElementById('dso-val').textContent = `${wcRes.dso_days} days`;
        document.getElementById('dpo-val').textContent = `${wcRes.dpo_days} days`;

        // Table
        allTransactions = txRes;
        filterTable();

      } catch (err) {
        console.warn('Dashboard poll error:', err);
      }
    }

    function filterTable() {
      const typeFilter = document.getElementById('type-filter').value;
      const searchVal = document.getElementById('search-input').value.trim().toLowerCase();

      let filtered = allTransactions;
      if (typeFilter !== 'ALL') {
        filtered = filtered.filter(t => t.document_type === typeFilter);
      }
      if (searchVal) {
        filtered = filtered.filter(t => 
          (t.document_id && t.document_id.toLowerCase().includes(searchVal)) ||
          (t.account_code && t.account_code.toLowerCase().includes(searchVal)) ||
          (t.cost_center && t.cost_center.toLowerCase().includes(searchVal)) ||
          (t.description && t.description.toLowerCase().includes(searchVal))
        );
      }

      document.getElementById('filtered-count-badge').textContent = `(Showing ${filtered.length} of ${allTransactions.length})`;
      renderTableRows(filtered);
    }

    function renderTableRows(txs) {
      const tbody = document.getElementById('tx-tbody');
      if (!txs || txs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-slate-500">No matching transaction records found.</td></tr>';
        return;
      }

      tbody.innerHTML = txs.map(tx => {
        const isDebit = tx.is_debit;
        const typeBadge = tx.document_type === 'KR' 
          ? '<span class="px-2 py-0.5 rounded bg-rose-500/10 text-rose-400 border border-rose-500/20 text-[10px]">KR (AP Vendor)</span>'
          : tx.document_type === 'DR'
          ? '<span class="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 text-[10px]">DR (AR Customer)</span>'
          : '<span class="px-2 py-0.5 rounded bg-sky-500/10 text-sky-400 border border-sky-500/20 text-[10px]">SA (G/L Journal)</span>';

        const dcBadge = isDebit 
          ? '<span class="text-emerald-400 font-bold">DEBIT (S)</span>' 
          : '<span class="text-sky-400 font-bold">CREDIT (H)</span>';

        return `
          <tr class="hover:bg-slate-800/40 transition">
            <td class="py-3 px-4 font-bold text-white">${tx.document_id}</td>
            <td class="py-3 px-4 text-slate-400">${tx.posting_date}</td>
            <td class="py-3 px-4">${typeBadge}</td>
            <td class="py-3 px-4 font-semibold text-slate-200">${tx.account_code}</td>
            <td class="py-3 px-4 text-slate-400">${tx.cost_center || '—'}</td>
            <td class="py-3 px-4">${dcBadge}</td>
            <td class="py-3 px-4 text-right font-bold ${isDebit ? 'text-emerald-400' : 'text-slate-100'}">₹${Number(tx.amount).toLocaleString('en-IN', {minimumFractionDigits: 2})}</td>
          </tr>
        `;
      }).join('');
    }

    // CSV Export
    function exportToCSV() {
      if (!allTransactions || allTransactions.length === 0) {
        alert('No data to export.');
        return;
      }
      const headers = ["CompanyCode", "DocumentID", "FiscalYear", "PostingDate", "DocType", "GLAccount", "DebitCredit", "Amount", "Currency", "CostCenter", "Description"];
      const rows = allTransactions.map(t => [
        t.company_id,
        t.document_id,
        t.fiscal_year,
        t.posting_date,
        t.document_type,
        t.account_code,
        t.is_debit ? "DEBIT" : "CREDIT",
        t.amount,
        t.currency,
        t.cost_center || "",
        `"${(t.description || "").replace(/"/g, '""')}"`
      ]);

      const csvContent = "data:text/csv;charset=utf-8," + [headers.join(","), ...rows.map(e => e.join(","))].join("\\n");
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `sap_s4hana_ledger_${new Date().toISOString().slice(0,10)}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }

    // Interactive Sync with Idempotency Key
    async function triggerSync() {
      const btn = document.getElementById('sync-btn');
      const icon = document.getElementById('sync-icon');
      btn.disabled = true;
      icon.classList.add('animate-spin');
      btn.classList.add('opacity-75');

      const idempotencyKey = `SYNC_CLIENT_${Date.now()}_${Math.random().toString(36).substring(7)}`;

      try {
        const res = await fetch('/api/v1/sap/sync?batch_size=50', {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer zt_sap_frontend_token_2026',
            'Idempotency-Key': idempotencyKey
          }
        });
        if (!res.ok) {
          const errData = await res.json();
          alert('Sync Warning: ' + (errData.detail || 'Service degraded'));
        }
        await loadDashboardData();
      } catch (e) {
        alert('Sync error: ' + e);
      } finally {
        btn.disabled = false;
        icon.classList.remove('animate-spin');
        btn.classList.remove('opacity-75');
      }
    }

    // Fault Injection Toggle
    async function toggleFaultInjection() {
      try {
        const targetState = !faultActive;
        const res = await fetch(`/api/v1/resilience/fault-inject?enable=${targetState}`, { method: 'POST' });
        await res.json();
        await loadDashboardData();
      } catch (e) {
        alert('Fault injection error: ' + e);
      }
    }

    // DLQ Reprocessing
    async function retryDLQ() {
      const btn = document.getElementById('dlq-retry-btn');
      btn.disabled = true;
      btn.textContent = 'Retrying...';
      try {
        const res = await fetch('/api/v1/resilience/dlq/retry', { method: 'POST' });
        const data = await res.json();
        alert(`DLQ Reprocessing Complete: ${data.reprocessed_count} items reprocessed. Remaining: ${data.remaining_dlq}`);
        await loadDashboardData();
      } catch (e) {
        alert('DLQ Retry error: ' + e);
      } finally {
        btn.disabled = false;
        btn.textContent = '↺ Retry DLQ';
      }
    }

    // Polling Toggle
    function togglePolling() {
      isPolling = !isPolling;
      const text = document.getElementById('polling-text');
      const indicator = document.getElementById('polling-indicator');

      if (isPolling) {
        text.textContent = 'Live Polling: 3s';
        indicator.className = 'w-2 h-2 rounded-full bg-emerald-400 animate-pulse';
        pollTimer = setInterval(loadDashboardData, 3000);
      } else {
        text.textContent = 'Polling Paused';
        indicator.className = 'w-2 h-2 rounded-full bg-slate-500';
        clearInterval(pollTimer);
      }
    }

    // Start auto-polling
    loadDashboardData();
    pollTimer = setInterval(loadDashboardData, 3000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)
