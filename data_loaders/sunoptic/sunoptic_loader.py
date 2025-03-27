import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from data_loaders.validation_utils import validate_file_format

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def load_master_sales_rep():
    """Load the master_sales_rep table from the database."""
    query = """
        SELECT "Source", "Customer field", "Data field value", "Sales Rep name", "Valid from", "Valid until"
        FROM master_sales_rep
        WHERE "Source" = 'Sunoptics'
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            master_df = pd.read_sql_query(query, conn)
        # Convert date columns to datetime
        master_df["Valid from"] = pd.to_datetime(master_df["Valid from"], errors='coerce')
        master_df["Valid until"] = pd.to_datetime(master_df["Valid until"], errors='coerce')
        return master_df
    except Exception as e:
        raise RuntimeError(f"Error loading master_sales_rep table: {e}")
    finally:
        engine.dispose()

def load_excel_file_sunoptic(filepath: str) -> pd.DataFrame:
    # Read the Excel file starting from the correct header row
    raw_df = pd.read_excel(filepath, header=0)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Sunoptic")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()
    
    # 1. Drop rows that have no values in key columns
    required_columns = [
        "Invoice ID", "Invoice Date", "Customer ID", "Bill Name", 
        "Sales Order ID", "Item ID", "Item Name", "Prod Fam", 
        "Unit Price", "Ship Qty", "Customer Type", "Ship To Name", 
        "Address Ship to", "Ship To City", "Ship To State"
    ]
    
    # Ensure all required columns exist
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    df = df.dropna(subset=required_columns, how='all')

    # 6. Convert "Commission %" from percentage to decimal factor
    if "Commission %" in df.columns:
        df["Commission %"] = df["Commission %"].astype(str).str.replace('%', '', regex=False)
        df["Commission %"] = pd.to_numeric(df["Commission %"], errors='coerce')

    # 2. Convert "Invoice Date" from m/d/YYYY to YYYY-MM-DD
    date_columns = ["Invoice Date"]
    for date_col in date_columns:
        if "Invoice Date" in df.columns:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
    
        # 4. Remove '$' from "Unit Price", "Line Amount", and "Commission $"
    monetary_columns = ["Unit Price", "Line Amount", "Commission $"]
    for col in monetary_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
            #df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 5. Convert "Ship Qty" to integers
    if "Ship Qty" in df.columns:
        df["Ship Qty"] = pd.to_numeric(df["Ship Qty"], errors='coerce').fillna(0).round(2)

    # ✅ Load the master data from the database
    master_df = load_master_sales_rep()

    # ✅ Enrich the DataFrame
    def enrich_sales_rep(name):
        # Convert empty strings to None before processing
        if pd.isna(name) or str(name).strip() == "":
            return None
            
        # Ensure we're using string comparison correctly
        match = master_df[
            (master_df["Source"] == "Sunoptics") &
            (master_df["Data field value"].str.strip() == str(name).strip())
        ]
        
        if not match.empty:
            return match.iloc[0]["Sales Rep name"]
        
        # Explicitly return None, not an empty string
        return None
    
    # After enrichment, ensure empty strings are converted to None
    if "Sales Rep Name" in df.columns:
        df["Sales Rep Name"] = df["Sales Rep Name"].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == "" else x
        )
    
    # Enrich the data
    if "Customer ID" in df.columns:
        df["Sales Rep Name"] = df["Customer ID"].apply(enrich_sales_rep)
        
        # # Ensure no None or empty values remain
        # df["Sales Rep Name"] = df["Sales Rep Name"].fillna("Unknown")
    
    return df