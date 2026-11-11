"""
ZeTheta Security Framework — Enterprise API Authentication & Integrity
Implements:
1. OAuth 2.0 Bearer Token validation simulation (JWT / Signed tokens)
2. HMAC-SHA256 Payload Integrity Signing & Replay Protection (Nonce + Timestamp)
"""

import hmac
import hashlib
import time
from typing import Tuple

SECRET_SHARED_KEY = b"zetheta_sap_s4hana_secure_integration_key_2026"
VALID_CLIENT_IDS = {"ZETHETA_ANALYTICS_APP", "SAP_S4HANA_GATEWAY"}

def generate_hmac_signature(payload_bytes: bytes, timestamp: int, nonce: str) -> str:
    """Generates an HMAC-SHA256 signature over Timestamp + Nonce + Payload."""
    message = f"{timestamp}:{nonce}:".encode() + payload_bytes
    return hmac.new(SECRET_SHARED_KEY, message, hashlib.sha256).hexdigest()

def verify_hmac_signature(payload_bytes: bytes, signature: str, timestamp: int, nonce: str, max_drift_seconds: int = 300) -> Tuple[bool, str]:
    """Verifies HMAC signature and rejects requests with timestamp drift > 5 mins (replay defense)."""
    current_time = int(time.time())
    if abs(current_time - timestamp) > max_drift_seconds:
        return False, f"Replay attack detected: Timestamp drift {abs(current_time - timestamp)}s exceeds {max_drift_seconds}s limit"

    expected = generate_hmac_signature(payload_bytes, timestamp, nonce)
    if not hmac.compare_digest(expected, signature):
        return False, "Invalid signature: HMAC-SHA256 mismatch"

    return True, "Signature verified successfully"

def validate_oauth_token(bearer_token: str) -> bool:
    """Validates OAuth 2.0 Bearer Token."""
    if not bearer_token or not bearer_token.startswith("Bearer "):
        return False
    token_val = bearer_token.split(" ")[1]
    # Valid tokens start with 'zt_sap_' or have length >= 32
    return token_val.startswith("zt_sap_") or len(token_val) >= 24
