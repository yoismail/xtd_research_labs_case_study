import shutil
import logging
import argparse
from pathlib import Path
from etl.logger import section, timed, setup_logging

# ✅ START LOGGING
setup_logging()

# ✅ Paths
BRONZE_FOLDER = Path("data/bronze")
SILVER_FOLDER = Path("data/silver")
GOLD_FOLDER = Path("data/gold/gold_carbon_historical.csv")
SPARK_TEMP = Path("C:/spark_temp")


def delete_file(path: Path):
    """Delete a single file safely."""
    if path.exists() and path.is_file():
        path.unlink()
        logging.info(f"🗑️ Deleted file: {path}")
    else:
        logging.info(f"⚠️ File not found (skipped): {path}")


def delete_folder(path: Path, keep_folder: bool = True):
    """
    Delete folder contents safely.
    :param keep_folder: If True, delete contents but keep the empty folder structure.
    """
    if path.exists() and path.is_dir():
        shutil.rmtree(path)
        logging.info(f"🗑️ Deleted folder: {path}")
        if keep_folder:
            path.mkdir(parents=True, exist_ok=True)
            logging.info(f"📂 Recreated empty folder: {path}")
    else:
        logging.info(f"⚠️ Folder not found (skipped): {path}")


@timed
def wipe(mode: str):
    section(f"🔷 🧹 Wiping — mode: {mode.upper()}")
    mode = mode.lower()

    if mode == "bronze":
        delete_folder(BRONZE_FOLDER)

    elif mode == "silver":
        delete_folder(SILVER_FOLDER)

    elif mode == "gold":
        # Delete the Spark output FOLDER, not a file
        delete_folder(GOLD_FOLDER)

    elif mode == "all":
        logging.info(
            "🔄 Safe Reset: Wiping Silver, Gold & Spark temp — keeping Bronze raw data")
        delete_folder(SILVER_FOLDER)
        delete_folder(GOLD_FOLDER)
        delete_folder(SPARK_TEMP)

    elif mode == "full":
        logging.warning(
            "💀 DANGER: WIPING EVERYTHING INCLUDING BRONZE RAW DATA 💀")
        delete_folder(BRONZE_FOLDER)
        delete_folder(SILVER_FOLDER)
        delete_folder(GOLD_FOLDER)
        delete_folder(SPARK_TEMP)

    else:
        logging.error(f"❌ Unknown mode: {mode}")
        return

    logging.info(
        f"\033[92m🎉 Wipe completed successfully for mode: {mode}\033[0m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="🧹 Wipe ETL data safely."
    )
    parser.add_argument(
        "mode",
        choices=["bronze", "silver", "gold", "all", "full"],
        help="bronze=raw | silver=parquet | gold=csv output | all=safe reset | full=delete everything"
    )
    args = parser.parse_args()
    wipe(args.mode)
