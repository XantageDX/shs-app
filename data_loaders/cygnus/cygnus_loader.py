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
        WHERE "Source" = 'Cygnus'
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

def load_excel_file_cygnus(filepath: str) -> pd.DataFrame:
    # Read the Excel file starting from the correct header row
    raw_df = pd.read_excel(filepath, header=3)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Cygnus")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()
    
    # Drop rows that are completely empty
    df.dropna(how='all', inplace=True)
    
    # Remove rows that contain "Total" in the first column
    if df.columns[0] not in (None, ""):
        df = df[~df.iloc[:, 0].astype(str).str.contains("Total", case=False, na=False)]
    
    # Remove rows where "Cust. ID" contains "total" (case-insensitive)
    if "Cust. ID" in df.columns:
        df = df[~df["Cust. ID"].astype(str).str.contains("total", case=False, na=False)]
    
    # Convert Rep % from strings like "7,0%" to floats like 0.07
    if "Rep %" in df.columns:
        df["Rep %"] = df["Rep %"].astype(str).str.replace("%", "", regex=False).str.replace(",", ".", regex=False)
        df["Rep %"] = pd.to_numeric(df["Rep %"], errors='coerce')
    
    # Split and format date columns while retaining "Inv Date", "Inv Date MM", and "Inv Date YYYY"
    date_columns = ["Inv Date", "Due Date", "ClosedDate"]
    for date_col in date_columns:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if date_col == "ClosedDate":
                df[f"{date_col} YYYY"] = df[date_col].dt.year.astype("Int64").astype(str).str.replace(",", "")
                df[f"{date_col} MM"] = df[date_col].dt.month.astype("Int64").astype(str).str.zfill(2)
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')

    # ✅ Move "Inv Date YYYY" and "Inv Date MM" right after "Inv Date"
    if "ClosedDate" in df.columns and "ClosedDate YYYY" in df.columns and "ClosedDate MM" in df.columns:
        cols = list(df.columns)
        cols.remove("ClosedDate YYYY")
        cols.remove("ClosedDate MM")
        index_of_inv_date = cols.index("ClosedDate")
        cols.insert(index_of_inv_date + 1, "ClosedDate YYYY")
        cols.insert(index_of_inv_date + 2, "ClosedDate MM")
        df = df[cols]

    # Convert numeric columns and ensure proper formatting
    for numeric_col in ["Invoice Total", "Total Rep Due"]:
        if numeric_col in df.columns:
            df[numeric_col] = (
                df[numeric_col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
            )
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors='coerce')
            df[numeric_col] = df[numeric_col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    # Ensure "Invoice" column is treated as a string without numeric interpretation
    if "Invoice" in df.columns:
        df["Invoice"] = df["Invoice"].ffill()
        df["Invoice"] = df["Invoice"].apply(
            lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, float) else str(x)
        )
        df["Invoice"] = df["Invoice"].str.strip()
        df = df[df["Invoice"] != ""]
        
    # Ensure "Sales Rep" column is filled with the value above if empty
    if "Sales Rep" in df.columns:
        df["Sales Rep"] = df["Sales Rep"].ffill().str.strip()

    # Ensure specified columns are filled with the value above if empty
    columns_to_fill = ["Cust. ID", "Cust- Name", "Name", "Address", "City", "State"]
    for column in columns_to_fill:
        if column in df.columns:
            df[column] = df[column].ffill().str.strip()

    # ✅ Load the master data from the database
    master_df = load_master_sales_rep()

    # ✅ Enrich the DataFrame
    def enrich_sales_rep(name):
        match = master_df[
            (master_df["Source"] == "Cygnus") & 
            (master_df["Data field value"].str.strip() == str(name).strip())
        ]
        if not match.empty:
            return match.iloc[0]["Sales Rep name"]
        return None

    if "Name" in df.columns:
        df["Sales Rep Name"] = df["Name"].apply(enrich_sales_rep)

    # ✅ Move the "Enriched" column right after "Sales Rep"
    if "Sales Rep" in df.columns and "Sales Rep Name" in df.columns:
        cols = list(df.columns)
        cols.remove("Sales Rep Name")
        index_of_sales_rep = cols.index("Sales Rep")
        cols.insert(index_of_sales_rep + 1, "Sales Rep Name")
        df = df[cols]

    return df
