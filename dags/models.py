from pydantic import BaseModel, Field, field_validator, field_validator
from typing import Annotated,Dict
import datetime
import dateutil.parser

# ==========================
#   PROCESSED METAL MODEL
# ==========================

#example:
"""
{
    "timestamp": "2025-12-12T01:38:03.570Z",
    "status": "success",
    "metals": {
      "gold": 137.1954,
      "silver": 2.0311,
      "platinum": 54.2414,
      "copper": 0.0121,
      "nickel": 0.0146
    },
    "currency_base": "USD"
}
"""
class ProcessedMetalModel(BaseModel):
    event_timestamp: datetime.datetime
    metal_symbol: str
    price_usd: Annotated[float, Field(gt=0)]
    currency_base: str
    unit: str
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    @field_validator('metal_symbol')
    @classmethod
    def validate_is_not_empty(cls, v):
        if not v:
            raise ValueError('metal symbol must not be empty')
        return v
    
# ==========================
#   CURRENCY RATE MODELS
# ==========================
# example:
"""
{
    "date": "2025-12-12 00:00:00+00",
    "base": "USD",
    "rates": {
        "IDR": "16663.0",
        "EUR": "0.9415",
        "CNY": "7.2025"
}
"""
class CurrencyRateModel(BaseModel):
    rate_date: datetime.datetime
    currency_code: str
    exchange_rate: Annotated[float, Field(gt=0)]
    base_currency: str
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    @field_validator('exchange_rate')
    @classmethod
    def validate_is_not_empty(cls, v):
        if not v:
            raise ValueError('exchange rate must not be empty')
        return v
    
    @field_validator('rate_date', mode='before')
    @classmethod
    def parse_flexible_date(cls, v):
        if isinstance(v, str):
            try:
                # dateutil.parser bisa handle "+00" dengan benar
                return dateutil.parser.parse(v)
            except Exception:
                # Fallback manual jika perlu
                return v.split('+')[0].strip()
        return v

# ===========================
#  FRED DATA MODEL
# ===========================
# example:
"""
{
  "realtime_start": "2026-01-28",
  "realtime_end": "2026-01-28",
  "observation_start": "2026-01-22",
  "observation_end": "2026-01-29",
  "units": "lin",
  "output_type": 1,
  "file_type": "json",
  "order_by": "observation_date",
  "sort_order": "asc",
  "count": 4,
  "offset": 0,
  "limit": 100000,
  "observations": [
    {
      "realtime_start": "2026-01-28",
      "realtime_end": "2026-01-28",
      "date": "2026-01-22",
      "value": "4.26"
    },
    {
      "realtime_start": "2026-01-28",
      "realtime_end": "2026-01-28",
      "date": "2026-01-23",
      "value": "4.24"
    },
    {
      "realtime_start": "2026-01-28",
      "realtime_end": "2026-01-28",
      "date": "2026-01-26",
      "value": "4.22"
    },
    {
      "realtime_start": "2026-01-28",
      "realtime_end": "2026-01-28",
      "date": "2026-01-27",
      "value": "4.24"
    }
  ]
}
"""
class FredDataModel(BaseModel):
    series_id: str
    observation_date: datetime.datetime
    observation_values: Annotated[float, Field(gt=0)]
    units: str
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    @field_validator('observation_values', mode='before')
    @classmethod
    def handle_fred_dots(cls, v):
        if v == "." or v is None:
            return None
        return float(v)
# ===========================
# NEWS COUNT MODEL
# ===========================
# example:
"""
{
    "status": "ok", 
    "totalResults": 1234
}
"""
class NewsCountModel(BaseModel):
    news_timestamp: datetime.datetime
    keywords: str
    total_mentions: Annotated[int, Field(ge=0)]
    status: str
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    @field_validator('status')
    @classmethod
    def validate_status_ok(cls, v):
        if v != "ok":
            raise ValueError('status must be "ok"')
        return v
