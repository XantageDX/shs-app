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
      3. Rename Date to Revenue Recognition Date.
      4. Add "Revenue Recognition Date YYYY" with the first 4 characters from "Revenue Recognition Date".
      5. Add "Revenue Recognition Date MM" with the 6th and 7th characters from "Revenue Recognition Date".
      6. Convert "Quantity" to floats with two decimal places.
      7. Convert "Total" and "Formula" to floats with two decimals.
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
    
    # 3. Convert "Date" to YYYY-MM-DD format and rename to "Revenue Recognition Date"
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y").dt.strftime("%Y-%m-%d")
    df["Revenue Recognition Date"] = df["Date"]
    
    # 4. Add "Revenue Recognition Date YYYY": first 4 characters from "Revenue Recognition Date" (the year).
    df["Revenue Recognition Date YYYY"] = df["Revenue Recognition Date"].str[:4]
    
    # 5. Add "Revenue Recognition Date MM": 6th and 7th characters from "Revenue Recognition Date" (the month).
    df["Revenue Recognition Date MM"] = df["Revenue Recognition Date"].str[5:7]
    
    # Reorder columns: Move Revenue Recognition Date fields together
    cols = df.columns.tolist()
    if "Date" in cols and "Revenue Recognition Date" in cols and "Revenue Recognition Date YYYY" in cols and "Revenue Recognition Date MM" in cols:
        cols.remove("Revenue Recognition Date")
        cols.remove("Revenue Recognition Date YYYY")
        cols.remove("Revenue Recognition Date MM")
        date_index = cols.index("Date")
        cols.insert(date_index + 1, "Revenue Recognition Date")
        cols.insert(date_index + 2, "Revenue Recognition Date YYYY")
        cols.insert(date_index + 3, "Revenue Recognition Date MM")
        df = df[cols]
        # Remove the original "Date" column after we've inserted the new columns correctly
        cols.remove("Date")
        df = df[cols]
    
    # 6. Convert "Quantity" to floats with two decimal places.
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").round(2)
    
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