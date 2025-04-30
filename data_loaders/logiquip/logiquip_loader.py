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
        WHERE "Source" = 'Logiquip'
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

def load_excel_file_logiquip(filepath: str) -> pd.DataFrame:
    # Read the Excel file with converters to preserve original format
    pd.set_option('display.max_columns', None)
    raw_df = pd.read_excel(filepath, header=1)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Logiquip")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()
    
    # Drop rows that are completely empty
    df.dropna(how='all', inplace=True)
    
    # Remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains("Unnamed", case=False)]
    
    # Remove rows that contain "Total" in specific columns (e.g., "Doc Num")
    if "Doc Num" in df.columns:
        df = df[~df["Doc Num"].astype(str).str.contains("Total", case=False, na=False)]
    
    # Ensure specified columns are filled with the value above if empty
    columns_to_fill = ["Rep", "Customer", "PO Number", "Ship To Zip", "Item Class"]
    for column in columns_to_fill:
        if column in df.columns:
            df[column] = df[column].astype(str).replace(["", "nan"], pd.NA).ffill().str.strip()
    
    # Handle NaN values in "Rep" column
    if "Rep" in df.columns:
        df["Rep"] = df["Rep"].apply(lambda x: str(int(float(x))) if pd.notnull(x)
                                         and isinstance(x, (int, float, str))
                                         and str(x).replace('.','',1).isdigit()
                       else str(x))

    # Convert "Comm Rate" from strings like "7,0%" to floats like 0.07
    if "Comm Rate" in df.columns:
        df["Comm Rate"] = (
            df["Comm Rate"].astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["Comm Rate"] = pd.to_numeric(df["Comm Rate"], errors='coerce') / 100
    
    # Fix the "Ship To Zip" column to ensure no trailing zeros
    if "Ship To Zip" in df.columns:
        df["Ship To Zip"] = df["Ship To Zip"].apply(lambda x: str(int(float(x))) if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x)
    
    # ✅ Handle "Date Paid" column and rename to "Revenue Recognition Date"
    if "Date Paid" in df.columns:
        # Convert "Date Paid" to datetime and handle errors
        df["Revenue Recognition Date"] = pd.to_datetime(df["Date Paid"], format="%m-%d-%Y", errors='coerce')
        
        # Forward-fill <NA> values in "Revenue Recognition Date"
        df["Revenue Recognition Date"] = df["Revenue Recognition Date"].fillna(method="ffill")
        
        # Create the reformatted "Revenue Recognition Date" column in "YYYY-MM-DD" format
        df["Revenue Recognition Date"] = df["Revenue Recognition Date"].dt.strftime('%Y-%m-%d')

        # Extract year and month from "Revenue Recognition Date"
        df["Revenue Recognition YYYY"] = pd.to_datetime(df["Revenue Recognition Date"], errors='coerce').dt.year.astype("Int64")
        df["Revenue Recognition MM"] = pd.to_datetime(df["Revenue Recognition Date"], errors='coerce').dt.month.apply(
            lambda x: f"{int(x):02d}" if pd.notnull(x) else ""
        )
        
        # Drop the original "Date Paid" column
        df = df.drop(columns=["Date Paid"])

    # Convert numeric columns and ensure proper formatting
    numeric_columns = ["Comm Amt", "Doc Amt"]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = (
                df[column].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
            )
            df[column] = pd.to_numeric(df[column], errors='coerce')
            df[column] = df[column].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
    
    # Remove rows where "Comm Rate" is empty
    if "Comm Rate" in df.columns:
        df = df.dropna(subset=["Comm Rate"])
    
    # Fill empty values in the "Agency" column with the upper non-empty value
    if "Agency" in df.columns:
        df["Agency"] = df["Agency"].astype(str).replace(["", "nan"], pd.NA).ffill().str.strip()

    # Forward-fill specific columns
    columns_to_inherit = ["Contract", "Revenue Recognition YYYY", "Revenue Recognition MM", "Ship To Zip", "PO Number", "Customer", "Doc Num"]
    existing_cols = [col for col in columns_to_inherit if col in df.columns]
    if existing_cols:
        for col in existing_cols:
            df[col] = df[col].astype(str).replace(["", "nan"], pd.NA)
        df[existing_cols] = df[existing_cols].ffill()

    if "Ship To Zip" in df.columns and "Customer" in df.columns:
        df["SteppingStone"] = df["Ship To Zip"] + "; " + df["Customer"]

    # ✅ Populate the "Sales Rep Name" column based on the master_sales_rep table
    master_df = load_master_sales_rep()

    def enrich_sales_rep(row):
        # Match "SteppingStone" and "Revenue Recognition Date" with master file criteria
        match = master_df[
            (master_df["Data field value"] == row["SteppingStone"]) &
            (master_df["Valid from"] <= row["Revenue Recognition Date"]) &
            ((master_df["Valid until"].isna()) | (master_df["Valid until"] >= row["Revenue Recognition Date"]))
        ]
        if not match.empty:
            return match.iloc[0]["Sales Rep name"]
        return None

    if "SteppingStone" in df.columns and "Revenue Recognition Date" in df.columns:
        df["Sales Rep Name"] = df.apply(enrich_sales_rep, axis=1)

    # ✅ Reorder columns to place "Sales Rep Name" before "SteppingStone"
    if "Sales Rep Name" in df.columns and "SteppingStone" in df.columns:
        cols = df.columns.tolist()
        cols.remove("Sales Rep Name")
        cols.insert(cols.index("SteppingStone"), "Sales Rep Name")
        df = df[cols]

    return df