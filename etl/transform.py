from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import logging
import sys
from etl.logger import setup_logging, section, timed
from etl.extract import BRONZE_DIR

# Configure Logging
setup_logging()

# Configure PySpark Environment
hadoop_home = "C:/hadoop"
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = os.path.join(
    hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Define directories
SILVER_DIR = "data/silver"   # Stores all processed silver data
GOLD_DIR = "data/gold"       # Stores final gold CSV output


def ensure_directories():
    """Ensure all required directories exist."""
    section("Ensuring Directories Exist")
    for directory in [SILVER_DIR, GOLD_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logging.info(f"Created directory: {directory}")
        else:
            logging.info(f"Directory already exists: {directory}")


def create_spark_session():
    section("Creating Spark Session")
    spark = SparkSession.builder \
        .appName("XTD_Labs_Historical_Data_Processing") \
        .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.4.jar") \
        .config("spark.driver.memory", "14g") \
        .config("spark.executor.memory", "14g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1024m")  \
        .config("spark.driver.host", "localhost") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark Session created successfully.")
    return spark


def get_new_bronze_data(spark):
    """
    Load ONLY new raw data that hasn't been added to Silver yet.
    Tracks processed files by name.
    """
    section("Checking for New Raw Data")

    # List all JSON files in Bronze
    all_files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".json")]
    if not all_files:
        logging.info("No files found in Bronze.")
        return None

    # Track which files have already been processed
    tracker_path = f"{SILVER_DIR}/_processed_files.txt"
    processed = []
    if os.path.exists(tracker_path):
        with open(tracker_path, "r") as f:
            # Read everything that is written inside inside the tracker path, and bring it all out as ONE LONG Text String, then split it into a list of new lines (each line is a filename)
            processed = f.read().splitlines()

    # Find new files
    new_files = [f for f in all_files if f not in processed]

    if not new_files:
        logging.info("No NEW raw data to process — everything up to date.")
        return None

    logging.info(
        f"Found {len(new_files)} NEW file(s) to process")

    # Load only new files
    paths = [f"{BRONZE_DIR}/{f}" for f in new_files]
    raw_df = spark.read.json(paths, multiLine=True)
    logging.info(f"Loaded {raw_df.count()} new records from Bronze.")

    # Mark these files as processed
    with open(tracker_path, "a") as f:
        f.writelines([line + "\n" for line in new_files])

    return raw_df


def transform_bronze_to_silver(raw_df):
    """
    Apply Silver transformations: flatten, explode, pivot.
    Same logic every time — reusable.
    """
    logging.info("Transforming data to Silver format...")

    exploded_df = raw_df.select(
        F.col("from").alias("timestamp"),
        F.explode(F.col("regions")).alias("region")
    )

    silver_df = exploded_df.select(
        F.col("timestamp"),
        F.col("region.regionid").alias("regionid"),
        F.col("region.shortname").alias('shortname'),
        F.col("region.dnoregion").alias('dno'),
        F.col("region.intensity.forecast").alias("intensity"),
        F.col("region.intensity.index").alias("index"),
        F.explode(F.col("region.generationmix")).alias("mix")
    )
    logging.info(f"Exploded generation mix for {silver_df.count()} records.")

    silver_df_pivoted = silver_df.groupBy(
        "regionid", "shortname", "dno", "timestamp", "intensity", "index"
    ).pivot("mix.fuel").agg(F.first("mix.perc"))

    logging.info(
        f"Pivoted generation mix for {silver_df_pivoted.count()} records.")

    return silver_df_pivoted


def silver_layer(spark):
    """
    Silver Layer:
    - If new data exists → process & APPEND to Silver
    - Always return FULL Silver dataset
    """
    section("Silver Layer — Incremental Mode")

    # Step 1: Get ONLY new raw data
    new_raw = get_new_bronze_data(spark)

    # Step 2: Process new data if exists
    if new_raw is not None:
        new_silver = transform_bronze_to_silver(new_raw)

        # Step 3: Append new data to Silver storage
        new_silver.write.parquet(f"{SILVER_DIR}/silver_data", mode="append")
        logging.info("New Silver data appended successfully.")

    # Step 4: Return FULL Silver dataset (old + new)
    full_silver = spark.read.parquet(f"{SILVER_DIR}/silver_data")
    logging.info(
        f"Full Silver dataset ready — {full_silver.count()} total records.")

    return full_silver


def gold_layer(silver_df_pivoted):
    """
    Transform FULL Silver → Gold daily aggregation.
    Runs every time on ALL available data.
    """
    section("Gold Layer Processing")
    logging.info("Aggregating to daily averages...")

    gold_df = silver_df_pivoted.withColumn("date_recorded", F.to_date("timestamp")) \
        .groupBy("regionid", "date_recorded") \
        .agg(
            F.first("shortname").alias("shortname"),
            F.first("dno").alias("dno"),
            F.round(F.mean("intensity"), 2).alias("intensity_avg"),
            F.mode("index").alias("index_mode"),
            F.round(F.mean("biomass"), 2).alias("fuel_biomass"),
            F.round(F.mean("coal"), 2).alias("fuel_coal"),
            F.round(F.mean("gas"), 2).alias("fuel_gas"),
            F.round(F.mean("hydro"), 2).alias("fuel_hydro"),
            F.round(F.mean("imports"), 2).alias("fuel_imports"),
            F.round(F.mean("nuclear"), 2).alias("fuel_nuclear"),
            F.round(F.mean("other"), 2).alias("fuel_other"),
            F.round(F.mean("solar"), 2).alias("fuel_solar"),
            F.round(F.mean("wind"), 2).alias("fuel_wind")
    ).orderBy("date_recorded", "regionid")

    logging.info(f"Gold layer ready — {gold_df.count()} daily records.")
    return gold_df


def save_gold_layer(gold_df):
    logging.info(f"Saving gold to: {GOLD_DIR}/gold_carbon_historical.csv")
    gold_df.write.csv(f"{GOLD_DIR}/gold_carbon_historical.csv",
                      header=True, mode="overwrite")

    gold_df.show(10)
    gold_df.printSchema()
    logging.info("Gold layer saved successfully.")


@timed
def main(spark):
    try:
        ensure_directories()

        # Silver: incremental, adds new data automatically
        silver_df = silver_layer(spark)

        # Gold: always fresh aggregation of ALL data
        gold_df = gold_layer(silver_df)

        save_gold_layer(gold_df)

        logging.info("FULL PIPELINE COMPLETED — ALL NEW DATA INCLUDED!")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark_session()
        main(spark)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info(
                "===================== END OF TRANSFORMATION =====================")
