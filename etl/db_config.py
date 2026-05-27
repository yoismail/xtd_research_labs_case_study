import os
from dotenv import load_dotenv

load_dotenv()

REQUIRED = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME"]
missing = [var_name for var_name in REQUIRED if not os.getenv(var_name)]
if missing:
    raise EnvironmentError(f"Missing required env vars: {missing}")

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME")
}
