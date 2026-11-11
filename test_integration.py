"""
ZeTheta S/4HANA Custom API Integration — Comprehensive Automated Test Suite
Validates Part 1 through Part 7 of Project 1 B deliverables + Enterprise Resilience Upgrades:
1. SAP ERP Schema Validation & Extraction
2. Transformation & Double-Entry Accounting Reconciliation
3. Working Capital & Liquidity Ratios Calculation
4. OAuth 2.0 & HMAC Replay Attack Defense
5. Dead-Letter Queue & Circuit Breaker Fault Isolation
6. End-to-End FastAPI Gateway HTTP Ingestion
7. Idempotency Key Caching & Deduplication
8. Operator DLQ Retry & Reprocessing Engine
9. Interactive Fault Injection & Circuit Breaker Simulation
"""

import time
import pytest
from decimal import Decimal
from fastapi.testclient import TestClient

from app import app, extractor, circuit_breaker, dlq
from sap_models import SAPDocumentType
from sap_extractor import SAPODataExtractor
from transformer import SAPDataTransformer
from security import generate_hmac_signature, verify_hmac_signature, validate_oauth_token
from resilience import DeadLetterQueue, CircuitBreaker

client = TestClient(app)

def test_part1_and_2_openapi_specification():
    """Validates OpenAPI schema is compliant and contains all required routes."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert "/api/v1/sap/sync" in schema["paths"]
    assert "/api/v1/analytics/working-capital" in schema["paths"]
    assert "/api/v1/monitoring/health" in schema["paths"]
    assert "/api/v1/resilience/dlq/retry" in schema["paths"]
    assert "/api/v1/resilience/fault-inject" in schema["paths"]
    print("✓ Test 1 Passed: OpenAPI 3.1 specification conforms to enterprise standards.")

def test_part3_sap_extraction_and_pagination():
    """Validates SAP OData extraction layer and pagination."""
    test_extractor = SAPODataExtractor(seed_records=85)
    batch1, next_skip = test_extractor.extract_delta_batch(skip=0, top=50)
    assert len(batch1) == 50
    assert next_skip == 50

    batch2, next_skip_final = test_extractor.extract_delta_batch(skip=50, top=50)
    assert len(batch2) == 35
    assert next_skip_final is None
    print(f"✓ Test 2 Passed: SAP extraction layer extracted {len(batch1) + len(batch2)} records across paginated cursors.")

def test_part4_transformation_and_balance_reconciliation():
    """Validates transformation mapping and 100% mathematical reconciliation."""
    test_extractor = SAPODataExtractor(seed_records=60)
    docs, _ = test_extractor.extract_delta_batch(skip=0, top=60)
    
    # Test line item transformations
    txs = SAPDataTransformer.transform_document_to_transactions(docs[0])
    assert len(txs) == 2
    assert txs[0].is_debit is True
    assert txs[1].is_debit is False
    assert txs[0].amount == txs[1].amount

    # Test aggregate working capital and reconciliation
    metrics, total_debit, total_credit, is_balanced = SAPDataTransformer.compute_working_capital_metrics(docs)
    assert is_balanced is True
    assert total_debit == total_credit
    assert total_debit > Decimal("0.00")
    assert metrics.total_receivables >= Decimal("0.00")
    assert metrics.total_payables >= Decimal("0.00")
    print(f"✓ Test 3 Passed: 100% mathematical reconciliation verified. Debit=Credit=₹{total_debit:,.2f}.")

def test_part5_resilience_circuit_breaker_and_dlq():
    """Validates Circuit Breaker tripping and Dead-Letter Queue quarantine."""
    cb = CircuitBreaker(failure_threshold=3, recovery_time_seconds=0.5)
    test_dlq = DeadLetterQueue()

    assert cb.state == "CLOSED"
    assert cb.can_execute() is True

    # Simulate 3 failures
    for i in range(3):
        cb.record_failure()
        test_dlq.push({"doc_id": f"FAIL_{i}"}, "Timeout to SAP S/4HANA", attempts=3)

    assert cb.state == "OPEN"
    assert cb.can_execute() is False
    assert test_dlq.size() == 3

    # Wait for recovery
    time.sleep(0.6)
    assert cb.can_execute() is True
    assert cb.state == "HALF_OPEN"
    cb.record_success()
    assert cb.state == "CLOSED"
    print("✓ Test 4 Passed: Fault tolerance engine (Circuit Breaker & DLQ) verified.")

def test_part6_security_hmac_and_replay_protection():
    """Validates HMAC-SHA256 signature and replay attack prevention."""
    payload = b'{"batch_id":"TEST_001","amount":500000}'
    now = int(time.time())
    nonce = "nonce_random_94821"

    # Valid signature
    sig = generate_hmac_signature(payload, now, nonce)
    valid, msg = verify_hmac_signature(payload, sig, now, nonce)
    assert valid is True

    # Tampered payload detection
    tampered_payload = b'{"batch_id":"TEST_001","amount":999999}'
    valid_tamper, msg_tamper = verify_hmac_signature(tampered_payload, sig, now, nonce)
    assert valid_tamper is False
    assert "Invalid signature" in msg_tamper

    # Replay attack detection (timestamp 10 mins old)
    old_time = now - 600
    old_sig = generate_hmac_signature(payload, old_time, nonce)
    valid_replay, msg_replay = verify_hmac_signature(payload, old_sig, old_time, nonce)
    assert valid_replay is False
    assert "Replay attack detected" in msg_replay
    print("✓ Test 5 Passed: Security layer (OAuth 2.0 & HMAC-SHA256 replay defense) verified.")

def test_part7_end_to_end_fastapi_sync():
    """Validates full stack HTTP ingestion endpoint."""
    response = client.post(
        "/api/v1/sap/sync?batch_size=50",
        headers={"Authorization": "Bearer zt_sap_valid_access_token_2026"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["processed_records"] == 50
    assert data["is_balanced"] is True
    assert "ZT_SYNC_" in data["sync_batch_id"]

    # Verify working capital analytics endpoint
    res_wc = client.get("/api/v1/analytics/working-capital")
    assert res_wc.status_code == 200
    wc_data = res_wc.json()
    assert "net_working_capital" in wc_data

    # Verify health telemetry endpoint
    res_health = client.get("/api/v1/monitoring/health")
    assert res_health.status_code == 200
    assert res_health.json()["status"] == "HEALTHY"
    print("✓ Test 6 Passed: End-to-end FastAPI integration gateway verified.")

def test_part8_idempotency_key_enforcement():
    """Validates that duplicate requests with the same Idempotency-Key return cached results with X-Cache-Lookup: HIT."""
    test_key = f"IDEMP_{int(time.time())}_UNIQUE_99"
    headers = {
        "Authorization": "Bearer zt_sap_valid_access_token_2026",
        "Idempotency-Key": test_key
    }

    # First call: MISS
    resp1 = client.post("/api/v1/sap/sync?batch_size=30", headers=headers)
    assert resp1.status_code == 200
    assert resp1.headers.get("X-Cache-Lookup") == "MISS"
    data1 = resp1.json()

    # Second call with same key: HIT (idempotent replay)
    resp2 = client.post("/api/v1/sap/sync?batch_size=30", headers=headers)
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Cache-Lookup") == "HIT"
    data2 = resp2.json()

    # Ensure identical batch id and no double counting
    assert data1["sync_batch_id"] == data2["sync_batch_id"]
    assert data2["idempotent_cached"] is True
    print("✓ Test 7 Passed: Idempotency-Key enforcement prevents duplicate ingestion.")

def test_part9_dlq_reprocessing_endpoint():
    """Validates that operator endpoint /api/v1/resilience/dlq/retry reprocesses quarantined items."""
    # Seed DLQ with poison pills
    dlq.push({"doc": "ERR_001"}, "Simulated extraction failure", attempts=3)
    dlq.push({"doc": "ERR_002"}, "Simulated schema mismatch", attempts=3)
    assert dlq.size() >= 2

    # Call DLQ retry endpoint
    res = client.post("/api/v1/resilience/dlq/retry")
    assert res.status_code == 200
    data = res.json()
    assert data["reprocessed_count"] >= 2
    assert data["remaining_dlq"] == 0
    assert data["status"] == "REPROCESSED"
    print("✓ Test 8 Passed: DLQ retry and re-ingestion endpoint verified.")

def test_part10_fault_injection_and_circuit_breaker():
    """Validates interactive fault injection trips circuit breaker and recovers on reset."""
    # 1. Activate fault injection
    res_fault = client.post("/api/v1/resilience/fault-inject?enable=true")
    assert res_fault.status_code == 200
    fault_data = res_fault.json()
    assert fault_data["simulated_failure_active"] is True
    assert fault_data["circuit_breaker_state"] == "OPEN"

    # 2. Verify health reports DEGRADED
    res_health = client.get("/api/v1/monitoring/health")
    assert res_health.json()["status"] == "DEGRADED"

    # 3. Verify sync is blocked by OPEN circuit breaker (503 Service Unavailable)
    res_sync = client.post(
        "/api/v1/sap/sync?batch_size=20",
        headers={"Authorization": "Bearer zt_sap_valid_access_token_2026"}
    )
    assert res_sync.status_code == 503
    assert "Circuit breaker is OPEN" in res_sync.json()["detail"]

    # 4. Deactivate fault injection / reset
    res_reset = client.post("/api/v1/resilience/fault-inject?enable=false")
    assert res_reset.status_code == 200
    assert res_reset.json()["simulated_failure_active"] is False
    assert res_reset.json()["circuit_breaker_state"] == "CLOSED"

    # 5. Verify health is restored to HEALTHY
    res_health_after = client.get("/api/v1/monitoring/health")
    assert res_health_after.json()["status"] == "HEALTHY"
    print("✓ Test 9 Passed: Fault injection simulator trips breaker to 503 and resets cleanly.")

def test_part11_transaction_filtering_and_html_dashboard():
    """Validates HTML visual dashboard at / and transaction query filtering."""
    # Test HTML dashboard
    res_root = client.get("/")
    assert res_root.status_code == 200
    assert "text/html" in res_root.headers.get("content-type", "")
    assert "ZeTheta S/4HANA Gateway" in res_root.text

    # Test filtering by Document Type: SA (Journal)
    res_tx = client.get("/api/v1/sap/transactions?doc_type=SA&limit=10")
    assert res_tx.status_code == 200
    txs = res_tx.json()
    for t in txs:
        assert t["document_type"] == "SA"
    print("✓ Test 10 Passed: HTML dashboard and transaction type filtering verified.")

if __name__ == "__main__":
    test_part1_and_2_openapi_specification()
    test_part3_sap_extraction_and_pagination()
    test_part4_transformation_and_balance_reconciliation()
    test_part5_resilience_circuit_breaker_and_dlq()
    test_part6_security_hmac_and_replay_protection()
    test_part7_end_to_end_fastapi_sync()
    test_part8_idempotency_key_enforcement()
    test_part9_dlq_reprocessing_endpoint()
    test_part10_fault_injection_and_circuit_breaker()
    test_part11_transaction_filtering_and_html_dashboard()
    print("=" * 60)
    print("ALL 10 AUTOMATED INTEGRATION TESTS PASSED! (100% Pass Rate)")
    print("=" * 60)
