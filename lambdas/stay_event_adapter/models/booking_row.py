# lambdas/stay_event_adapter/models/booking_row.py

from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

class BookingRow(BaseModel):
    rewards_id: Optional[str]
    reservation_id: Optional[str]
    property_id: Optional[str]
    arrival_dt_key: Optional[int]
    departure_dt_key: Optional[int]
    rate_code: Optional[str]
    dim_dist_channel_3_key: Optional[str]
    cancel_dt_key: Optional[int] = None
    dim_dist_channel_1_key: Optional[str] = None

class Config:
    extra = "ignore"

