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

def load_excel_file_inspektor(filepath: str) -> pd.DataFrame:
    """
    Load and transform an Inspektor Excel file into a pandas DataFrame.
    
    Processing steps:
      1. Drop the column "Sales Rep".
      2. Drop rows where "Name" is empty.
      3. Convert "Date" column from m/d/YYYY to YYYY-MM-DD.
      4. Add "Date YYYY" column with the first 4 characters from "Date".
      5. Add "Date MM" column with the 6th and 7th characters from "Date".
         (Both "Date YYYY" and "Date MM" will be inserted right after the "Date" column)
      6. Convert "Quantity" to integers.
      7. Convert "Total" and "Formula" to floats with two decimals.
         (Original format: "$9,646.20" -> 9646.20)
      8. Convert "Commission %" from percentage to factor.
      9. Rename "Name" column to "Sales Rep Name".
    """
    # Read the Excel file starting from the correct header row
    raw_df = pd.read_excel(filepath, header=0)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "InspeKtor")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()

    # 1. Drop the column "Sales Rep" if it exists.
    if "Sales Rep" in df.columns:
        df = df.drop(columns=["Sales Rep"])
    
    # 2. Drop rows where "Name" is empty (handles NaN and empty strings).
    df = df.dropna(subset=["Name"])
    df["Name"] = df["Name"].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df = df[df["Name"].astype(str).str.strip() != ""]
    
    # 3. Convert "Date" from m/d/YYYY to YYYY-MM-DD.
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y").dt.strftime("%Y-%m-%d")
    
    # 4. Add "Date YYYY": first 4 characters from "Date" (the year).
    df["Date YYYY"] = df["Date"].str[:4]
    
    # 5. Add "Date MM": 6th and 7th characters from "Date" (the month).
    df["Date MM"] = df["Date"].str[5:7]
    
    # Reorder columns: Move "Date YYYY" and "Date MM" right after "Date"
    cols = df.columns.tolist()
    if "Date" in cols and "Date YYYY" in cols and "Date MM" in cols:
        cols.remove("Date YYYY")
        cols.remove("Date MM")
        date_index = cols.index("Date")
        cols.insert(date_index + 1, "Date YYYY")
        cols.insert(date_index + 2, "Date MM")
        df = df[cols]
    
    # 6. Convert "Quantity" to integers.
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")
    
    # 7. Convert "Total" and "Formula" to floats with two decimals.
    for col in ["Total", "Formula"]:
        if col in df.columns:
            # Remove dollar signs and thousand separators (commas)
            df[col] = df[col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    
    # 8. Convert "Commission %" from percentage to a factor (e.g., "7.0%" -> 0.07).
    if "Commission %" in df.columns:
        df["Commission %"] = df["Commission %"].astype(str).str.replace("%", "", regex=False)
        df["Commission %"] = pd.to_numeric(df["Commission %"], errors="coerce") / 100
    
    # 9. Rename "Name" column to "Sales Rep Name".
    df = df.rename(columns={"Name": "Sales Rep Name"})
    
    return df
