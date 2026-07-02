# Asychonous data extraction from the API
import logging
import aiohttp
import asyncio
import json
import os
from datetime import date, timedelta
from etl.logger import section, setup_logging, timed


# Initialize logging
setup_logging()


# folder path for storing the extracted data
BRONZE_DIR = "data/bronze"


def ensure_bronze_dir():
    """Ensure the bronze data directory exists."""
    section("Ensuring Bronze Directory Exists")
    if not os.path.exists(BRONZE_DIR):
        os.makedirs(BRONZE_DIR, exist_ok=True)
        logging.info(f"Created bronze data directory: {BRONZE_DIR}")
    else:
        logging.info(f"Bronze data directory already exists: {BRONZE_DIR}")


semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
# Asynchronous function to fetch data from the API


async def fetch_carbon_data(session, target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    file_path = os.path.join(BRONZE_DIR, f"{target_date}.json")

    if os.path.exists(file_path):
        logging.info(
            f"Data for {target_date} already exists. Skipping API call.")
        return None

    try:
        async with semaphore:
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=20) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Check if API actually returned data (sometimes returns empty)
                            if 'data' in data and data['data']:
                                with open(file_path, "w", encoding="utf-8") as f:
                                    json.dump(data['data'], f,
                                              ensure_ascii=False, indent=4)
                                logging.info(
                                    f"Successfully saved {target_date}")
                            else:
                                logging.warning(
                                    f"No data returned for {target_date}")
                            return data

                        elif response.status == 429:
                            logging.warning(
                                f"Rate limited for {target_date}. Attempt {attempt+1}/3")
                            await asyncio.sleep(5 * (attempt + 1))
                        else:
                            logging.error(
                                f"HTTP {response.status} for {target_date}")
                            await asyncio.sleep(2)  # Wait before retry

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logging.error(
                        f"Network error {target_date} attempt {attempt+1}: {e}")
                    await asyncio.sleep(2 * (attempt + 1))

            # Only reached if all 3 attempts failed
            logging.error(
                f"FAILED COMPLETELY: {target_date} after 3 attempts")
            return None

    except Exception as e:
        logging.error(f"Exception fetching {target_date}: {e}")
        return None


# Controller for  Coroutine execution

async def run_extraction():
    section("Starting Data Extraction from API")
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)
    current = start_date

    # Creates an empty list to store all the API requests we want to run.
    # Purpose: We collect all tasks first, then run them together.
    tasks = []

    # Apply rate limiting to reduce simultaneous connections (this ultimately prevents IP bans)
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        while current <= end_date:
            tasks.append(fetch_carbon_data(session, current))
            current += timedelta(days=1)
        # Runs all the tasks in the list at the same time (up to our limit of 5).
        await asyncio.gather(*tasks)


# Count the number of JSON files in the bronze directory
def count_json_files():
    section("Counting Extracted JSON Files")
    json_count = len([f for f in os.listdir(BRONZE_DIR)
                      if os.path.isfile(os.path.join(BRONZE_DIR, f))
                      and f.endswith('.json')])

    logging.info(f"Total JSON files: {json_count}")


@timed
def main():
    section("Starting Extraction Process")
    ensure_bronze_dir()
    asyncio.run(run_extraction())
    count_json_files()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
    finally:
        logging.info(
            "===================== END OF EXTRACTION =====================")
