from pydantic import BaseModel, Field, field_validator, field_validator
from typing import Annotated,Dict
import datetime

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
    timestamp: str
    status: str
    metals: Dict[str, Annotated[float, Field(gt=0)]]
    currency_base: str

    @field_validator('metals')
    @classmethod
    def validate_is_not_empty(cls, v):
        if not v:
            raise ValueError('metals dictionary must not be empty')
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
    date: str
    base: str
    rates: Dict[str, Annotated[float, Field(gt=0)]]

    @field_validator('rates')
    @classmethod
    def validate_is_not_empty(cls, v):
        if not v:
            raise ValueError('rates dictionary must not be empty')
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
    realtime_start: str
    realtime_end: str
    observation_start: str
    observation_end: str
    units: str
    output_type: int
    file_type: str
    order_by: str
    sort_order: str
    count: Annotated[int, Field(ge=0)]
    offset: Annotated[int, Field(ge=0)]
    limit: Annotated[int, Field(ge=0)]
    # the dict must contain date and value keys
    observations: Annotated[list[Dict[str, str]], Field(min_length=1)]

    @field_validator('observations')
    @classmethod
    def validate_observations_not_empty(cls, v):
        if not v:
            raise ValueError('observations list must not be empty')
        for obs in v:
            if 'date' not in obs or 'value' not in obs:
                raise ValueError('each observation must contain date and value keys')
        return v
    
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
    status: str
    totalResults: int = Field(...,ge=0)

    @field_validator('status')
    @classmethod
    def validate_status_ok(cls, v):
        if v != "ok":
            raise ValueError('status must be "ok"')
        return v
    
    # apa artinya annotated di atas?
    # Annotated[int, Field(ge=0)] berarti bahwa totalResults harus berupa integer yang nilainya lebih besar dari atau sama dengan 0 (ge = greater than or equal to). 

    # harus dibungkus Annotated? kalau iya kenapa?
    # Ya, harus dibungkus dengan Annotated karena ini adalah cara untuk menambahkan metadata
    # ke tipe data dasar (dalam hal ini int) menggunakan Pydantic. Dengan menggunakan Annotated,
    # kita dapat menerapkan validasi tambahan pada field tersebut, seperti memastikan bahwa
    # totalResults tidak negatif.

    # emang gk bisa cuman field(ge=0) aja?
    # Tidak, tidak bisa. Field(ge=0) sendiri tidak cukup karena Pydantic perlu tahu tipe data dasar yang sedang divalidasi.
    # Dengan menggunakan Annotated, kita memberi tahu Pydantic bahwa totalResults adalah sebuah integer
    # dan juga menerapkan aturan validasi bahwa nilainya harus lebih besar dari atau sama dengan 0.
    # kode di atas ga bisa emang?

