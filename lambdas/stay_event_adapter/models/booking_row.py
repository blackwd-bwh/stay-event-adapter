from dataclasses import dataclass, field, asdict
from typing import Optional
from decimal import Decimal

def to_serializable(value):
    if isinstance(value, Decimal):
        # Convert to int if there's no fractional part, else to float
        return int(value) if value == int(value) else float(value)
    return value

def safe_asdict(obj):
    return {k: to_serializable(v) for k, v in asdict(obj).items()}

@dataclass
class BookingRow:
    rewards_id: Optional[str] = None
    resv_detail_id: Optional[str] = None
    property_id: Optional[str] = None
    arrival_dt_key: Optional[int] = None
    departure_dt_key: Optional[int] = None
    rate_code: Optional[str] = None
    dim_dist_channel_3_key: Optional[str] = None
    cancel_dt_key: Optional[int] = None
    dim_dist_channel_1_key: Optional[str] = None

    @staticmethod
    def from_dict(data: dict) -> 'BookingRow':
        return BookingRow(**data)

    def to_dict(self) -> dict:
        return asdict(self)
