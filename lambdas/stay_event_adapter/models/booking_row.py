"""Data model representing a booking row fetched from Redshift."""

from dataclasses import dataclass, asdict, fields
from typing import Optional
from decimal import Decimal

@dataclass
class BookingRow:  # pylint: disable=too-many-instance-attributes
    """Container for all fields returned by the stay-completed query."""
    resv_nbr: Optional[str] = None
    resv_detail_id: Optional[str] = None
    booking_dt_key: Optional[str] = None
    arrival_dt_key: Optional[str] = None
    departure_dt_key: Optional[str] = None
    cancel_dt_key: Optional[str] = None
    conf_nbr: Optional[str] = None
    nbr_adults: Optional[int] = None
    nbr_children: Optional[int] = None
    dim_property_key: Optional[str] = None
    property_id: Optional[str] = None
    dist_channel_0: Optional[str] = None
    rewards_id: Optional[str] = None
    dim_ta_key: Optional[str] = None
    ta_id: Optional[str] = None
    dim_ca_key: Optional[str] = None
    ca_id: Optional[str] = None
    dim_rate_code_key: Optional[str] = None
    dim_rewards_key: Optional[int] = None
    rate_code: Optional[str] = None
    dim_room_type_key: Optional[str] = None
    room_category: Optional[str] = None
    dim_country_origin_key: Optional[str] = None
    country_origin_code: Optional[str] = None
    record_update_dttm: Optional[str] = None
    stay: Optional[int] = None
    stay_before_cancellation: Optional[int] = None
    length_of_stay: Optional[int] = None
    length_of_stay_before_cancellation: Optional[int] = None
    rev_local_curr: Optional[float] = None
    rev_usd: Optional[float] = None
    rev_before_cancellation_local_curr: Optional[float] = None
    rev_before_cancellation_usd: Optional[float] = None
    roomnights: Optional[int] = None
    roomnights_before_cancellation: Optional[int] = None
    adr_usd: Optional[float] = None
    adr_local_curr: Optional[float] = None
    adr_before_cancellation_usd: Optional[float] = None
    adr_before_cancellation_local_curr: Optional[float] = None
    vat_usd: Optional[float] = None
    vat_local_curr: Optional[float] = None
    vat_usd_departure: Optional[float] = None
    vat_local_curr_departure: Optional[float] = None
    dim_dist_channel_1_key: Optional[int] = None
    dim_dist_channel_2_key: Optional[int] = None
    dim_dist_channel_3_key: Optional[int] = None
    dim_dist_channel_4_key: Optional[int] = None
    dim_business_source_key: Optional[int] = None
    business_source_code: Optional[str] = None
    booking_exchange_rate: Optional[float] = None
    booking_currency_code: Optional[str] = None
    property_currency_exchange_rate: Optional[float] = None
    property_currency_code: Optional[str] = None
    name_id: Optional[int] = None
    ota_enroll_ind: Optional[str] = None
    wh_conf_nbr: Optional[str] = None
    sx_ind: Optional[str] = None
    lynx_ad_code: Optional[str] = None
    operator_user_id: Optional[int] = None
    guarantee_code: Optional[str] = None
    rate_code_original: Optional[str] = None
    dim_rate_code_original_key: Optional[str] = None
    dim_parent_acct_key: Optional[int] = None
    dim_sales_mgr_key: Optional[int] = None
    comm_ind: Optional[str] = None
    dim_operator_worker_key: Optional[int] = None
    operator_worker_id: Optional[str] = None
    dim_rate_header_market_segment_key: Optional[int] = None
    dim_update_operator_worker_key: Optional[int] = None
    update_operator_worker_id: Optional[str] = None
    gds_record_locator: Optional[str] = None
    source_record_update_dttm: Optional[str] = None
    dim_guest_key: Optional[int] = None
    guest_id: Optional[int] = None
    cancel_by_date: Optional[str] = None
    leg_no: Optional[int] = None
    original_leg_no: Optional[int] = None
    booking_dttm: Optional[str] = None
    dim_guest_departure_dt_key: Optional[int] = None
    batch_ind: Optional[str] = None
    batch_update_dttm: Optional[str] = None
    rev_usd_fx: Optional[float] = None
    rev_local_curr_fx: Optional[float] = None
    day_use_ind: Optional[str] = None
    no_show_ind: Optional[str] = None
    external_reference: Optional[str] = None
    crx_resv_ind: Optional[str] = None

    @staticmethod
    def from_dict(data: dict) -> 'BookingRow':
        """
        Build a BookingRow from a dictionary, converting Decimals to float
        and dropping unexpected fields.
        """
        field_names = {f.name for f in fields(BookingRow)}
        filtered = {
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in data.items()
            if k in field_names
        }
        return BookingRow(**filtered)

    def to_dict(self) -> dict:
        """Return a dictionary representation of the dataclass."""

        return asdict(self)

def safe_asdict(obj: BookingRow) -> dict:
    """Serialize a BookingRow to a dict converting Decimals to floats."""

    return {
        k: float(v) if isinstance(v, Decimal) else v
        for k, v in asdict(obj).items()
    }
