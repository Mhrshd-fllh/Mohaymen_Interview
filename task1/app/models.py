from datetime import datetime, timezone
from sqlalchemy import String, DateTime, Index, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

class CityCountry(Base):
    __tablename__ = "city_country"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
        comment="Primary Key autoincrement identifier"
    )

    city_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="City name identifier"
    )

    country_code: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="Country code associated with the city"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
        comment="Timestamp when record was created"
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
        comment="Timestamp when record was last updated"
    )


    __table_args__ = (
        Index(
            "ix_city_country_city_name_lower",
            text("LOWER(city_name)"),
            unique=True
        ),
    )

    def __repr__(self) -> str:
        return f"<CityCountry(id={self.id}, city_name='{self.city_name}', country_code='{self.country_code}')>"