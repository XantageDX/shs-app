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

def load_excel_file_sunoptic(filepath: str) -> pd.DataFrame:
    """
    Load and transform a Sunoptic Excel file into a pandas DataFrame.
    
    Processing steps:
      1. Drop rows that have no values in key columns.
      2. Convert "Invoice Date" from m/d/YYYY to YYYY-MM-DD.
      3. Add "Invoice Date YYYY" and "Invoice Date MM" columns right after "Invoice Date".
      4. Remove '$' from "Unit Price", "Line Amount", and "Commission $".
      5. Convert "Ship Qty" to integers.
      6. Convert "Commission %" from percentage to decimal factor.
    """
    # Read the Excel file
    raw_df = pd.read_excel(filepath, header=0)
    
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Sunoptic")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    # Proceed with the cleaning and enrichment
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

    # Rename "Sales Rep Name" -> "Company Sales Rep Name", then create a new "Sales Rep Name"
    if "Sales Rep Name" in df.columns:
        df.rename(columns={"Sales Rep Name": "Company Sales Rep Name"}, inplace=True)
        df["Sales Rep Name"] = (
            df["Company Sales Rep Name"]
            .astype(str)
            .str.strip()
        )
    
    # 2. Convert "Invoice Date" from m/d/YYYY to YYYY-MM-DD
    if "Invoice Date" in df.columns:
        df["Invoice Date"] = pd.to_datetime(df["Invoice Date"], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # 3. Add "Invoice Date YYYY" and "Invoice Date MM" right after "Invoice Date"
    if "Invoice Date" in df.columns:
        # Create the new columns
        df["Invoice Date YYYY"] = pd.to_datetime(df["Invoice Date"], errors='coerce').dt.year
        df["Invoice Date MM"] = pd.to_datetime(df["Invoice Date"], errors='coerce').dt.month.apply(
            lambda x: f"{int(x):02d}" if pd.notnull(x) else ""
        )
        
        # Reorder columns to place the new columns right after "Invoice Date"
        cols = df.columns.tolist()
        date_index = cols.index("Invoice Date")
        cols.remove("Invoice Date YYYY")
        cols.remove("Invoice Date MM")
        cols.insert(date_index + 1, "Invoice Date YYYY")
        cols.insert(date_index + 2, "Invoice Date MM")
        df = df[cols]
    
    # 4. Remove '$' from "Unit Price", "Line Amount", and "Commission $"
    monetary_columns = ["Unit Price", "Line Amount", "Commission $"]
    for col in monetary_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
            #df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 5. Convert "Ship Qty" to integers
    if "Ship Qty" in df.columns:
        df["Ship Qty"] = pd.to_numeric(df["Ship Qty"], errors='coerce').fillna(0).astype('int64')
    
    # 6. Convert "Commission %" from percentage to decimal factor
    if "Commission %" in df.columns:
        df["Commission %"] = df["Commission %"].astype(str).str.replace('%', '', regex=False)
        df["Commission %"] = pd.to_numeric(df["Commission %"], errors='coerce')
    
    return df