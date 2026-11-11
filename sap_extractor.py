"""
ZeTheta SAP Extractor — S/4HANA OData & RFC Extraction Engine
Simulates SAP S/4HANA OData API (`/sap/opu/odata/sap/API_FINANCIALTRANSACTION_SRV`):
- General Ledger (`A_JournalEntry`)
- Accounts Payable (`A_SupplierInvoice`)
- Accounts Receivable (`A_CustomerInvoice`)
- Delta loads with timestamp filtering (`$filter=LastModifiedDateTime gt ...`)
- Server-side cursor pagination (`$skiptoken`, `$top`)
"""

import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Tuple
from sap_models import SAPDocumentRaw, SAPGLItemRaw, SAPDocumentType

class SAPODataExtractor:
    def __init__(self, seed_records: int = 150):
        self.mock_database: List[SAPDocumentRaw] = self._generate_synthetic_sap_ledger(seed_records)

    def _generate_synthetic_sap_ledger(self, count: int) -> List[SAPDocumentRaw]:
        docs = []
        base_date = date.today() - timedelta(days=30)
        
        for i in range(1, count + 1):
            doc_num = f"10{i:08d}"
            doc_date = base_date + timedelta(days=(i % 30))
            is_vendor = (i % 3 == 0)
            is_customer = (i % 3 == 1)
            
            doc_type = SAPDocumentType.GL_JOURNAL
            if is_vendor:
                doc_type = SAPDocumentType.VENDOR_INVOICE
            elif is_customer:
                doc_type = SAPDocumentType.CUSTOMER_INVOICE

            amount = Decimal(str(5000 + (i * 750) % 85000))
            
            # Line Item 1 (Debit)
            item1 = SAPGLItemRaw(
                BUKRS="1000",
                BELNR=doc_num,
                GJAHR=2026,
                BUZEI=1,
                HKONT="400000" if not is_vendor else "600000",
                SHKZG="S",
                DMBTR=amount,
                HWAER="INR",
                KOSTL=f"CC_{100 + (i % 5)}",
                SGTXT=f"Operational transaction ref #{i}"
            )

            # Line Item 2 (Credit - balancing)
            item2 = SAPGLItemRaw(
                BUKRS="1000",
                BELNR=doc_num,
                GJAHR=2026,
                BUZEI=2,
                HKONT="110000" if is_vendor else "210000",
                SHKZG="H",
                DMBTR=amount,
                HWAER="INR",
                KOSTL=f"CC_{100 + (i % 5)}",
                SGTXT=f"Offset balancing entry #{i}"
            )

            doc = SAPDocumentRaw(
                BUKRS="1000",
                BELNR=doc_num,
                GJAHR=2026,
                BLART=doc_type,
                BLDAT=doc_date,
                BUDAT=doc_date,
                USNAM="SAP_RFC_BATCH",
                BKTXT=f"Auto batch extract {doc_num}",
                ITEMS=[item1, item2]
            )
            docs.append(doc)
            
        return docs

    def extract_delta_batch(
        self,
        since_date: date = None,
        skip: int = 0,
        top: int = 50
    ) -> Tuple[List[SAPDocumentRaw], Optional[int]]:
        """
        Simulates SAP S/4HANA OData extraction with pagination ($top, $skip) and delta filtering.
        Returns (records, next_skip_token)
        """
        filtered = self.mock_database
        if since_date:
            filtered = [d for d in filtered if d.BUDAT >= since_date]

        batch = filtered[skip : skip + top]
        next_skip = skip + top if (skip + top) < len(filtered) else None
        return batch, next_skip
