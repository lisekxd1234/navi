from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass(frozen=True)
class SellerConfig:
    name: str
    address_lines: tuple[str, ...]
    bank_account: str | None = None
    extra_info: Dict[str, str] | None = None


SELLER = SellerConfig(
    name="NaviUnlock Pro",
    address_lines=(
        "Sportowa 7B",
        "66-540 Stare Kurowo",
    ),
    bank_account=None,
    extra_info=None,
)

DEFAULT_ISSUE_PLACE = "Stare Kurowo"


UPLOAD_ROOT = Path(__file__).resolve().parent / "uploads"
UPLOAD_INVOICES = UPLOAD_ROOT / "invoices"
UPLOAD_NDG = UPLOAD_ROOT / "ndg"
