import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from etl.db_config import DB_CONFIG
from etl.logger import setup_logging, section, timed
from etl.transform import GOLD_DIR  # = "data/gold"

# Output path for transformed CSV files
OUTPUT_DIR = Path(GOLD_DIR) / "gold_carbon_historical.csv" / "*.csv"


def get_db_engine():
    """Create SQLAlchemy engine using values from .env"""
    section("Creating Database Engine")
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 60}
        )
        logging.info("Database engine created successfully")
        return engine
    except Exception as e:
        logging.error(f"Failed to create engine: {e}")
        return None


def ensure_schema_exists(engine, schema_name="carbon_data"):
    """Automatically create schema if it does not exist"""
    section("Ensuring Database Schema Exists")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
        logging.info(f"Schema {schema_name} is ready")
    except Exception as e:
        logging.error(f"Failed to create schema {schema_name}: {e}")
        sys.exit(1)


def load_to_postgres(df, table_name, engine) -> bool:
    """Load Gold layer DataFrame into PostgreSQL — safe, deduplicated, type‑correct"""
    section("Loading Data to Database")
    try:
        # PRIMARY KEY COLUMNS
        unique_cols = ["regionid", "date_recorded"]

        # STEP 1: DEDUPLICATE INPUT FILES FIRST (fast, in-memory)
        before_file = len(df)
        df = df.drop_duplicates(subset=unique_cols, keep="first")
        logging.info(
            f"Removed {before_file - len(df)} duplicate rows FROM INPUT FILES")

        if df.empty:
            logging.info("No valid rows left - nothing to load")
            return True

        # STEP 2: GET EXISTING KEYS FROM DB
        # Read date as DATE type directly
        existing = pd.read_sql(
            f"""
            SELECT regionid, date_recorded
            FROM carbon_data.{table_name}
            """,
            engine
        )
        # Make absolutely sure both sides are datetime64[ns]
        df["date_recorded"] = pd.to_datetime(df["date_recorded"])
        existing["date_recorded"] = pd.to_datetime(existing["date_recorded"])

        # STEP 3: REMOVE WHAT'S ALREADY THERE
        before_db = len(df)
        df = df.merge(existing, on=unique_cols, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
        logging.info(
            f"Skipped {before_db - len(df)} rows ALREADY IN DATABASE")

        if df.empty:
            logging.info("All rows already exist - table up to date")
            return True

        # STEP 4: APPEND NEW ROWS TO DB
        df.to_sql(
            table_name,
            engine,
            schema="carbon_data",
            if_exists="append",
            index=False,
            chunksize=20000
        )

        logging.info(f"SUCCESS: {len(df)} NEW rows loaded into {table_name}")
        return True

    except Exception as e:
        logging.exception(f"Failed to load data: {e}")
        return False


@timed
def main():
    setup_logging()
    engine = get_db_engine()
    if not engine:
        sys.exit(1)

    ensure_schema_exists(engine)

    # READ CSVS INTO PANDAS (handles all part-*.csv files in the gold directory)
    logging.info(f"Reading data from: {OUTPUT_DIR}")
    csv_files = list(Path(GOLD_DIR).glob(
        "gold_carbon_historical.csv/part-*.csv"))
    if not csv_files:
        logging.error("No part-*.csv files found!")
        sys.exit(1)

    # CRUCIAL: parse_dates makes Pandas treat it as real date objects
    df = pd.concat(
        [pd.read_csv(f, parse_dates=["date_recorded"]) for f in csv_files],
        ignore_index=True
    )
    logging.info(f"Total rows read from files: {len(df)}")

    load_to_postgres(df, "carbon_intensity_daily", engine)

    logging.info(
        "===================== END OF LOAD PROCESS =====================")


# RUN THE LOAD PROCESS
if __name__ == "__main__":
    main()
