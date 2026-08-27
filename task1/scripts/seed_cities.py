import argparse
import asyncio
import csv
import logging
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple

import httpx
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("seed_cities")

def load_cities_csv(csv_filepath: Path) -> List[Dict[str, str]]:
    csv_filepath = Path(csv_filepath)
    if not csv_filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_filepath}")


    records: List[Dict[str, str]] = []

    with open(csv_filepath, mode="r", encoding="utf-8-sig") as f:
        # Detect delimiter dynamically (default to comma)
        sample = f.read(2048)
        f.seek(0)

        delimiter = ","
        if ";" in sample and sample.count(";") > sample.count(","):
            delimiter = ";"
        elif "\t" in sample and sample.count("\t") > sample.count(","):
            delimiter = "\t"

        reader = csv.DictReader(f, delimiter=delimiter)
        raw_fieldnames = [str(name).strip() for name in (reader.fieldnames or [])]

        city_field = None
        country_field = None

        for field in raw_fieldnames:
            clean_field = "".join(c for c in field.lower() if c.isalnum())
            if clean_field in ("city", "cityname", "city_name"):
                city_field = field
            elif clean_field in ("countycode", "countrycode", "country_code", "country"):
                country_field = field

        # Fallback heuristic if exact clean match wasn't found
        if not city_field:
            city_field = next((f for f in raw_fieldnames if "city" in f.lower()), None)
        if not country_field:
            country_field = next((f for f in raw_fieldnames if "count" in f.lower() or "code" in f.lower()), None)

        if not city_field or not country_field:
            raise ValueError(
                f"Invalid CSV structure in '{csv_filepath}'. "
                f"Found columns {raw_fieldnames}. Required columns for city and country code."
            )

        for row in reader:
            city_val = row.get(city_field, "").strip() if city_field else ""
            country_val = row.get(country_field, "").strip() if country_field else ""

            if city_val and country_val:
                records.append({
                    "city": city_val,
                    "country_code": country_val
                })

    logger.info("Successfully loaded %d records from CSV dataset: %s", len(records), csv_filepath)
    return records

async def send_upsert_request(client: httpx.AsyncClient, api_url: str, record: Dict[str, str],
                      semaphore: asyncio.Semaphore, retries: int = 3) -> Tuple[bool, Optional[str]]:
    async with semaphore:
        attempt = 0
        while attempt < retries:
            try:
                response = await client.post(
                    api_url,
                    json=record,
                    timeout = 10.0
                )     

                if response.status_code == 200:
                    data = response.json()
                    msg = data.get("message", "ok")
                    return True, msg

                elif response.status_code >= 500:
                    attempt += 1
                    await asyncio.sleep(0.1 * (2 ** attempt))

                else:
                    return False, f"HTTP {response.status_code}: {response.text}"

            except (httpx.RequestError, httpx.TimeoutException) as exc:
                attempt += 1
                if attempt >= retries:
                    return False, f"Network error after {retries} attempts: {str(exc)}"

                await asyncio.sleep(0.1 * (2 ** attempt))

        return False, f"Failed after {retries} retries"

async def seed_database(csv_path: Path, api_url: str, concurrency: int = 50, batch_size: int = 500) -> None:
    records = load_cities_csv(csv_path)
    total_records = len(records)

    if total_records == 0:
        logger.warning("No records found in CSV file in {csv_path}")
        return

    logger.info(
        f"Starting ingestion: Total records={total_records}, concurrency={concurrency}, API URL={api_url}"
    )

    limits = httpx.Limits(
        max_keepalive_connections=concurrency,
        max_connections=concurrency * 2
    )

    semaphore = asyncio.Semaphore(concurrency)

    start_time = time.perf_counter()
    success_count = 0
    failure_count = 0

    async with httpx.AsyncClient(limits = limits) as client:
        tasks = [
            send_upsert_request(client, api_url, record, semaphore)
            for record in records
        ]

        for i in range(0, total_records, batch_size):
            chunk_tasks = tasks[i: i + batch_size]
            results = await asyncio.gather(*chunk_tasks)

            for success, msg in results:
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    if failure_count <= 5:
                        logger.error(f"Upsert failed: {msg}")

            processed = min(i + batch_size, total_records)
            elapsed = time.perf_counter() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            logger.info(
                "Progress: %d/%d (%.1f%%) | Success: %d | Failed: %d | Rate: %.1f req/sec",
                processed, total_records, (processed / total_records) * 100,
                success_count, failure_count, rate
            )

    total_elapsed = time.perf_counter() - start_time
    avg_rate = total_records / total_elapsed if total_elapsed > 0 else 0

    logger.info("==========================================================")
    logger.info("SEEDING COMPLETE SUMMARY")
    logger.info("==========================================================")
    logger.info("Total Records Processed: %d", total_records)
    logger.info("Successful Upserts:     %d", success_count)
    logger.info("Failed Upserts:         %d", failure_count)
    logger.info("Total Elapsed Time:     %.2f seconds", total_elapsed)
    logger.info("Average Throughput:     %.2f requests/second", avg_rate)
    logger.info("==========================================================")


def main():
    default_csv = Path(__file__).resolve().parent.parent.parent / "Cities" / "CountryCode-City.csv"
    default_url = "https://localhost:8000/cities"

    parser = argparse.ArgumentParser(description="Async Batch Seeding for Cities Dataset")

    parser.add_argument(
        "--csv-path",
        type=Path,
        default=default_csv,
        help=f"Path to CountryCode-City.csv (default: {default_csv})"
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default=default_url,
        help=f"FastAPI endpoint URL for POST /cities (default: {default_url})"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Maximum concurrent HTTP requests allowed (default: 50)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Progress logging chunk size (default: 500)"
    )

    args = parser.parse_args()
    api_url = args.api_url
    if api_url.startswith("https://localhost") or api_url.startswith("https://127.0.0.1"):
        logger.warning("Local FastAPI server runs on plain HTTP. Auto-correcting %s -> %s", api_url, api_url.replace("https://", "http://"))
        api_url = api_url.replace("https://", "http://")


    asyncio.run(
        seed_database(
            csv_path=args.csv_path,
            api_url=api_url,
            concurrency=args.concurrency,
            batch_size=args.batch_size
        )
    )


if __name__ == "__main__":
    main()