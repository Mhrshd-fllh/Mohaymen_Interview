from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func, select, text

from app.database import get_db_session
from app.models import CityCountry
from app.schemas import CityUpsertRequest, CityResponse

router = APIRouter(prefix="/cities", tags=["Cities"])

@router.post("", status_code=status.HTTP_201_CREATED, 
             summary="Create or update a City-Country record", 
             response_model=CityResponse, 
             description="This endpoint allows you to insert a new city-country record or update an existing one based on the city name. If the city already exists, its country code will be updated.")
async def upsert_city(payload: CityUpsertRequest, db: AsyncSession = Depends(get_db_session)) -> CityResponse:
    """
    Upsert a city-country record in the database.

    - **city**: Name of the city (case-insensitive).
    - **country_code**: Country code associated with the city.

    If the city already exists, its country code will be updated. If it doesn't exist, a new record will be created.
    """
    try:
        existing_stmt = select(CityCountry).where(func.lower(CityCountry.city_name) == func.lower(payload.city))
        existing_result = await db.execute(existing_stmt)
        existing_record = existing_result.scalar_one_or_none()
        is_created = existing_record is None

        upper_stmt = (
            insert(CityCountry).values(
                city_name=payload.city,
                country_code=payload.country_code
            ).on_conflict_do_update(
                constraint="ix_city_country_city_name_lower",
                index_elements = [text("LOWER(city_name)")],
                set_={"country_code": payload.country_code, "updated_at": func.now()}
            ).returning(CityCountry)
        )

        result = await db.execute(upper_stmt)
        upserted_record = result.scalar_one()

        await db.commit()

        return CityResponse(
            id=upserted_record.id,
            city_name=upserted_record.city_name,
            country_code=upserted_record.country_code,
            created_at=upserted_record.created_at,
            updated_at=upserted_record.updated_at,
            message="created" if is_created else "updated"
        )
            

    except:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database query failed.")