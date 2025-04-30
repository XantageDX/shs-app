import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from data_loaders.validation_utils import validate_file_format

# Load environment variables
load_dotenv()

# Build the database URL from env vars
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def get_db_connection():
    """Create a SQLAlchemy engine for the database."""
    return create_engine(DATABASE_URL)

def load_master_sales_rep() -> pd.DataFrame:
    """
    Load the master_sales_rep table (filtered to Source='Chemence')
    and parse its date columns.
    """
    query = """
        SELECT
            "Source",
            "Customer field",
            "Data field value",
            "Sales Rep name",
            "Valid from",
            "Valid until"
        FROM master_sales_rep
        WHERE "Source" = 'Chemence'
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            master_df = pd.read_sql_query(query, conn)
        master_df["Valid from"] = pd.to_datetime(master_df["Valid from"], errors="coerce")
        master_df["Valid until"] = pd.to_datetime(master_df["Valid until"], errors="coerce")
        return master_df
    except Exception as e:
        raise RuntimeError(f"Error loading master_sales_rep: {e}")
    finally:
        engine.dispose()

def load_excel_file_chemence(filepath: str) -> pd.DataFrame:
    """
    Load and transform a Chemence Excel file into a pandas DataFrame.
    Ensures all numeric columns are shown with two decimals (e.g. 0.00, 2.30, 0.10).
    
    Commission Date columns will be populated by sales_data_upload.py from user input.
    """
    # 1. Read & validate
    df = pd.read_excel(filepath, header=3)
    is_valid, missing = validate_file_format(df, "Chemence")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    df = df.copy()
    df.dropna(how="all", inplace=True)

    # 2. REMOVED - Commission Date logic
    # We're no longer extracting Commission Date from the file
    # This will be added by the sales_data_upload.py from the user's selection
    
    # 3. Revenue Recognition Date logic
    if "Invoice Date" in df.columns:
        # Rename Invoice Date to Revenue Recognition Date
        df["Revenue Recognition Date"] = df["Invoice Date"].astype(str)
        
        # Extract year and month parts
        if pd.api.types.is_datetime64_any_dtype(df["Invoice Date"]):
            # If it's already a datetime
            df["Revenue Recognition Date YYYY"] = df["Invoice Date"].dt.year.astype(str)
            df["Revenue Recognition Date MM"] = df["Invoice Date"].dt.month.apply(lambda m: f"{m:02d}")
        else:
            # If it's a string, extract the parts
            df["Revenue Recognition Date YYYY"] = df["Revenue Recognition Date"].str[:4]
            df["Revenue Recognition Date MM"] = df["Revenue Recognition Date"].str[-2:]
        
        # Drop the original Invoice Date column
        df = df.drop(columns=["Invoice Date"])

    # 4. Numeric conversion & rounding
    numeric_columns = ["Qty Shipped", "Sales Price", "Sales Total", "Commission", "Unit Price"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                        .str.replace(r"[\$,]", "", regex=True)
                        .pipe(pd.to_numeric, errors="coerce")
                        .fillna(0.0)
                        .round(2)
            )

    # 5. Compute Comm %
    if {"Commission", "Sales Total"}.issubset(df.columns):
        df["Comm %"] = (
            df["Commission"].div(df["Sales Total"])
                         .fillna(0)
                         .round(2)
        )

    # 6. Force two-decimal formatting on all floats (turns them into strings)
    float_cols = df.select_dtypes(include="float").columns
    df[float_cols] = df[float_cols].applymap(lambda x: f"{x:.2f}")

    # 7. Format revenue recognition date if it's a datetime
    if "Revenue Recognition Date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["Revenue Recognition Date"]):
        df["Revenue Recognition Date"] = df["Revenue Recognition Date"].dt.strftime("%Y-%m-%d")

    # 8. Ensure text columns are clean strings
    text_columns = [
        "Source", "Sales Group", "Source ID", "Account Number", "Account Name",
        "Street", "City", "State", "Zip", "Description", "Part #", "UOM", "Agreement"
    ]
    for col in text_columns:
        if col in df.columns:
            if col in {"Source ID", "Account Number"}:
                df[col] = (
                    df[col]
                    .apply(lambda x: str(int(float(x))) if pd.notnull(x)
                                           and isinstance(x, (int, float, str))
                                           and str(x).replace('.','',1).isdigit()
                           else str(x))
                    .replace("nan", "")
                    .str.strip()
                )
            else:
                df[col] = df[col].astype(str).replace("nan", "").str.strip()

    # 9. Enrich with Sales Rep Name lookup
    try:
        master_df = load_master_sales_rep()
        def lookup_sales_rep(source_id):
            if pd.isna(source_id) or source_id == "":
                return ""
            matches = master_df[
                (master_df["Customer field"] == "Source ID") &
                (master_df["Data field value"] == str(source_id))
            ]
            return matches.iloc[0]["Sales Rep name"] if not matches.empty else ""
        if "Source ID" in df.columns:
            df["Sales Rep Name"] = df["Source ID"].apply(lookup_sales_rep)
        else:
            df["Sales Rep Name"] = ""
    except Exception as e:
        print(f"Error enriching Sales Rep Name: {e}")
        df["Sales Rep Name"] = ""

    # 10. Reorder columns to place date fields together and create empty Commission Date placeholders
    # Create empty Commission Date columns that will be populated by sales_data_upload.py
    df["Commission Date"] = ""
    df["Commission Date YYYY"] = ""
    df["Commission Date MM"] = ""
    
    desired_columns = [
        "Source", "Commission Date", "Commission Date YYYY", "Commission Date MM",
        "Sales Group", "Source ID", "Sales Rep Name", "Account Number", "Account Name",
        "Street", "City", "State", "Zip", "Description", "Part #", 
        "Revenue Recognition Date", "Revenue Recognition Date YYYY", "Revenue Recognition Date MM",
        "Qty Shipped", "UOM", "Sales Price", "Sales Total", "Commission", "Unit Price",
        "Comm %", "Agreement"
    ]
    for col in desired_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[desired_columns]

    return df