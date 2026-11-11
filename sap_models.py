"""
ZeTheta Financial Analytics — SAP S/4HANA Data Contracts
Defines Pydantic v2 schemas for SAP ERP Financial Modules:
- FI-GL: General Ledger Accounting (BKPF Header, BSEG Line Items)
- FI-AP: Accounts Payable (Vendor Invoices, Aging)
- FI-AR: Accounts Receivable (Customer Invoices, Open Items)
- CO-OM: Cost Center Overhead Cost Controlling
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

class SAPDocumentType(str, Enum):
    GL_JOURNAL = "SA"
    VENDOR_INVOICE = "KR"
    VENDOR_PAYMENT = "KZ"
    CUSTOMER_INVOICE = "DR"
    CUSTOMER_PAYMENT = "DZ"

class SAPPostingStatus(str, Enum):
    POSTED = "POSTED"
    PARKED = "PARKED"
    REVERSED = "REVERSED"

# --- Raw SAP S/4HANA Schemas (Extraction Layer) ---

class SAPGLItemRaw(BaseModel):
    BUKRS: str = Field(..., description="Company Code (e.g. '1000')")
    BELNR: str = Field(..., description="Accounting Document Number")
    GJAHR: int = Field(..., description="Fiscal Year")
    BUZEI: int = Field(..., description="Line item number within document")
    HKONT: str = Field(..., description="G/L Account Number")
    SHKZG: str = Field(..., description="Debit/Credit Indicator: S=Debit, H=Credit")
    DMBTR: Decimal = Field(..., description="Amount in Local Currency")
    HWAER: str = Field("INR", description="Currency Key")
    KOSTL: Optional[str] = Field(None, description="Cost Center")
    SGTXT: Optional[str] = Field(None, description="Item Text / Narration")

class SAPDocumentRaw(BaseModel):
    BUKRS: str = Field(..., description="Company Code")
    BELNR: str = Field(..., description="Document Number")
    GJAHR: int = Field(..., description="Fiscal Year")
    BLART: SAPDocumentType = Field(..., description="Document Type")
    BLDAT: date = Field(..., description="Document Date")
    BUDAT: date = Field(..., description="Posting Date")
    USNAM: str = Field(..., description="User Name / Operator")
    BKTXT: Optional[str] = Field(None, description="Document Header Text")
    ITEMS: List[SAPGLItemRaw] = Field(default_factory=list, description="Associated Line Items")

# --- Normalized Analytics Target Schemas (Transformation Layer) ---

class NormalizedFinancialTransaction(BaseModel):
    company_id: str
    document_id: str
    fiscal_year: int
    posting_date: date
    document_type: str
    account_code: str
    is_debit: bool
    amount: Decimal
    currency: str
    cost_center: Optional[str]
    description: str

class WorkingCapitalMetrics(BaseModel):
    total_receivables: Decimal
    total_payables: Decimal
    net_working_capital: Decimal
    cash_ratio: Decimal
    dso_days: float = Field(..., description="Days Sales Outstanding")
    dpo_days: float = Field(..., description="Days Payable Outstanding")

class AnalyticsSummaryResponse(BaseModel):
    sync_batch_id: str
    timestamp: datetime
    processed_records: int
    total_debit_volume: Decimal
    total_credit_volume: Decimal
    is_balanced: bool
    idempotent_cached: bool = False
    working_capital: WorkingCapitalMetrics

class DLQRetryResponse(BaseModel):
    reprocessed_count: int
    remaining_dlq: int
    status: str
    timestamp: datetime

class FaultInjectionResponse(BaseModel):
    simulated_failure_active: bool
    circuit_breaker_state: str
    dlq_count: int
    message: str
