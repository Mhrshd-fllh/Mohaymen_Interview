from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, AliasChoices, field_validator

class CityUpsertRequest(BaseModel):
    city: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="City name to be inserted or updated in the database.",
        examples=["Tehran", "Ditroit", "Frankfurt"]
    )

    country_code: str = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Country code associated with the city.",
        examples=["IR", "US", "DE"]
    )

    @field_validator("city", mode="before")
    @classmethod
    def strip_and_clean_city(cls, value: str) -> str:
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("City name cannot be empty or whitespace.")
            return cleaned
        return value

    @field_validator("country_code", mode="before")
    @classmethod
    def normalize_country_code(cls, value: str) -> str:
        if isinstance(value, str):
            cleaned = value.strip().upper()
            if not cleaned:
                raise ValueError("Country code cannot be empty or whitespace.")
            return cleaned
        return value

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "city": "Tehran",
                "country_code": "IR"
            }
        }
    )


class CityResponse(BaseModel):
    id: int
    city_name: str
    country_code: str
    created_at: datetime
    updated_at: datetime
    message: Optional[str] = Field(default = None, description="Optional message providing additional context about the response.")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "city_name": "Tehran",
                "country_code": "IR",
                "created_at": "2023-10-01T12:00:00Z",
                "updated_at": "2023-10-01T12:00:00Z",
                "message": "City record retrieved successfully."
            }
        }
    )
    