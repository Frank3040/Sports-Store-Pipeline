import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import os
from src.logger import setup_logger

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"

LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / f"{SCRIPT_PATH.stem}.log"
logger = setup_logger(name=SCRIPT_PATH.stem, log_file=LOG_FILE)

DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres") 
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = "sports_store"

# Connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def transform_data():
    """
    Reads data from the raw schema, normalizes it into a star schema, 
    and loads it into the processed schema.
    """
    engine = None
    try:
        logger.info(f"Establishing connection to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
        engine = create_engine(DATABASE_URL)
        
        # Test connection
        with engine.connect() as conn:
            logger.info("Database connection successful.")

        logger.info("Reading data from raw.sales_raw...")
        df = pd.read_sql_table('sales_raw', con=engine, schema='raw')
        
        logger.info(f"Data read successfully. Rows: {df.shape[0]}, Columns: {df.shape[1]}")

        # --- 1. Initial Cleaning & Standardization ---
        rename_map = {
            "Retailer": "retailer",
            "Retailer ID": "retailer_id",
            "Invoice Date": "invoice_date",
            "Region": "region",
            "State": "state",
            "City": "city",
            "Product": "product",
            "Price per Unit": "price_per_unit",
            "Units Sold": "units_sold",
            "Total Sales": "total_sales",
            "Operating Proft": "operating_profit", 
            "Operating Margin": "operating_margin",
            "Sales Method": "sales_method"
        }
        df.rename(columns=rename_map, inplace=True)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        # Date Transformation: Ensure ISO 8601 format (YYYY-MM-DD)
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])
        logger.info("Date column transformed to datetime objects.")

        # --- 2. Normalization ---

        # A. Retailer Table
       
        dim_retailer = df[['retailer_id', 'retailer']].drop_duplicates().copy()
        dim_retailer.rename(columns={'retailer': 'retailer_name'}, inplace=True)
        
        # B. Region Table
        dim_region = df[['region']].drop_duplicates().reset_index(drop=True)
        dim_region['region_id'] = dim_region.index + 1
        dim_region.rename(columns={'region': 'region_name'}, inplace=True)

        # C. State Table
        df_state_prep = df[['state', 'region']].drop_duplicates()
        df_state_prep = df_state_prep.merge(dim_region, left_on='region', right_on='region_name', how='left')
        
        dim_state = df_state_prep[['state', 'region_id']].drop_duplicates().reset_index(drop=True)
        dim_state['state_id'] = dim_state.index + 1
        dim_state.rename(columns={'state': 'state_name'}, inplace=True)

        # D. City Table
        df_city_prep = df[['city', 'state']].drop_duplicates()
        df_city_prep = df_city_prep.merge(dim_state, left_on='state', right_on='state_name', how='left')
        
        dim_city = df_city_prep[['city', 'state_id']].drop_duplicates().reset_index(drop=True)
        dim_city['city_id'] = dim_city.index + 1
        dim_city.rename(columns={'city': 'city_name'}, inplace=True)

        # E. Product Table
        dim_product = df[['product', 'price_per_unit']].drop_duplicates(subset=['product']).reset_index(drop=True)
        dim_product['product_id'] = dim_product.index + 1
        dim_product.rename(columns={'product': 'product_name'}, inplace=True)

        # F. Sales Method Table
        dim_sales_method = df[['sales_method']].drop_duplicates().reset_index(drop=True)
        dim_sales_method['sales_method_id'] = dim_sales_method.index + 1
        dim_sales_method.rename(columns={'sales_method': 'method_name'}, inplace=True)

        # G. Fact Table: Sales Transactions
        # TransactionID (PK), RetailerID, InvoiceDate, CityID, ProductID, UnitsSold, TotalSales, OperatingProfit, OperatingMargin, SalesMethodID
        
        # Join everything back to the main dataframe to get IDs
        fact_df = df.copy()
        
        fact_df = fact_df.merge(dim_state, left_on='state', right_on='state_name', how='left')
        fact_df = fact_df.merge(dim_city, left_on=['city', 'state_id'], right_on=['city_name', 'state_id'], how='left')
        
        # Join for ProductID
        fact_df = fact_df.merge(dim_product, left_on='product', right_on='product_name', how='left')
        
        # Join for SalesMethodID
        fact_df = fact_df.merge(dim_sales_method, left_on='sales_method', right_on='method_name', how='left')

        # Join RegionID - REMOVED because region_id is already present from the dim_state merge
        
        # Select and Rename columns for Fact Table
        fact_table = fact_df[[
            'retailer_id', 
            'invoice_date', 
            'city_id', 
            'product_id', 
            'units_sold', 
            'total_sales', 
            'operating_profit', 
            'operating_margin', 
            'sales_method_id',
            'region_id'
        ]].copy()
        
        # Add Transaction ID
        fact_table['transaction_id'] = fact_table.index + 1
        
        # Reorder columns to match requirement: TransactionID first
        fact_table = fact_table[[
            'transaction_id', 'retailer_id', 'invoice_date', 'city_id', 'product_id', 
            'units_sold', 'total_sales', 'operating_profit', 'operating_margin', 'sales_method_id', 'region_id'
        ]]

        logger.info("Normalization complete. Starting load to 'processed' schema...")

        # --- 3. Loading to Database ---
        schema = "processed"
        
        # Helper to load table
        def load_table(dataframe, table_name, dtypes=None):
            logger.info(f"Loading table: {schema}.{table_name} ({len(dataframe)} rows)...")
            dataframe.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='replace',
                index=False,
                chunksize=1000,
                method='multi',
                dtype=dtypes
            )
            # Add Primary Key constraint (Postgres specific, optional but good practice)
            
        # Define types for safer loading
        load_table(dim_retailer, "retailer", {"retailer_id": String, "retailer_name": String})
        load_table(dim_region, "region", {"region_id": Integer, "region_name": String})
        load_table(dim_state, "state", {"state_id": Integer, "state_name": String, "region_id": Integer})
        load_table(dim_city, "city", {"city_id": Integer, "city_name": String, "state_id": Integer})
        load_table(dim_product, "product", {"product_id": Integer, "product_name": String, "price_per_unit": Integer})
        load_table(dim_sales_method, "sales_method", {"sales_method_id": Integer, "method_name": String})
        
        fact_dtypes = {
            "transaction_id": Integer,
            "retailer_id": String,
            "invoice_date": Date,
            "city_id": Integer,
            "product_id": Integer,
            "units_sold": Integer,
            "total_sales": Integer,
            "operating_profit": Integer,
            "operating_margin": Float,
            "sales_method_id": Integer,
            "region_id": Integer
        }
        load_table(fact_table, "sales_transaction", fact_dtypes)
        
        logger.info("All tables loaded successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if engine:
            engine.dispose()

if __name__ == "__main__":
    transform_data()
