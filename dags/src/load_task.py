import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float, Date
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import os
from src.logger import setup_logger

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data" / "raw"
FILE_NAME = "sport_products_sales_analysis_challenge_raw.xlsx"
FILE_PATH = DATA_DIR / FILE_NAME

LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / f"{SCRIPT_PATH.stem}.log"
logger = setup_logger(name=SCRIPT_PATH.stem, log_file=LOG_FILE)

# --- Database Configuration ---
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres") 
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = "sports_store"

# Connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_data():
    """
    Loads the Excel dataset into the PostgreSQL database.
    """
    if not FILE_PATH.exists():
        logger.error(f"File not found at: {FILE_PATH}")
        return

    engine = None
    try:
        logger.info(f"Establishing connection to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
        engine = create_engine(DATABASE_URL)
        
        # Test connection
        with engine.connect() as conn:
            logger.info("Database connection successful.")

        logger.info(f"Reading Excel file: {FILE_PATH}")
        # Read Excel file
        df = pd.read_excel(FILE_PATH)
        
        logger.info(f"File read successfully. Rows: {df.shape[0]}, Columns: {df.shape[1]}")

        table_name = "sales_raw"
        schema = "raw"

        logger.info(f"Starting insertion into {schema}.{table_name}...")
        
        # Define datatypes for SQLAlchemy
        dtype_mapping = {
            "Retailer": String,
            "Retailer ID": String,
            "Invoice Date": Date,
            "Region": String,
            "State": String,
            "City": String,
            "Product": String,
            "Price per Unit": Integer,
            "Units Sold": Integer,
            "Total Sales": Integer,
            "Operating Proft": Integer,
            "Operating Margin": Float,
            "Sales Method": String
        }

        # Use chunksize to handle large datasets efficiently during insertion
        # method='multi' allows for multiple rows per INSERT statement, significantly faster
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace', # Options: 'fail', 'replace', 'append'
            index=False,
            chunksize=1000, 
            method='multi',
            dtype=dtype_mapping
        )
        
        logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}.")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if engine:
            engine.dispose()

if __name__ == "__main__":
    load_data()
