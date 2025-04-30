import os
import re
import pandas as pd
import camelot
from sqlalchemy import create_engine
from dotenv import load_dotenv
from data_loaders.validation_utils import validate_file_format

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def extract_tables_from_pdf(pdf_file_path):
    """Extract tables from PDF using Camelot."""
    tables = camelot.read_pdf(pdf_file_path, pages="all", flavor="stream")
    if not tables:
        raise ValueError("No tables were found in the PDF using Camelot.")
    
    # Combine all tables into a single DataFrame
    df_list = [table.df for table in tables]
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

def clean_extracted_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and format the extracted DataFrame by dropping empty rows/columns."""
    # Drop completely empty rows and columns
    cleaned_df = raw_df.dropna(how="all").dropna(axis=1, how="all")
    
    # Assign placeholder column names (Column_0, Column_1, etc.)
    cleaned_df.columns = [f"Column_{i}" for i in range(cleaned_df.shape[1])]

    # Reset index for better readability
    cleaned_df.reset_index(drop=True, inplace=True)
    return cleaned_df

def format_table_logic_and_update_df(cleaned_df: pd.DataFrame):
    """
    Apply custom row‐dropping and date‐extraction logic, then rename columns.
    For PDFs, Revenue Recognition Date fields are populated from the PDF data.
    Commission Date fields will be added later in sales_data_upload.py.
    """
    date = None
    date_mm = None
    date_yyyy = None
    rows_to_drop = []

    # ----------------------
    # Identify date + row drops with 3 consecutive empty cells in Column_0
    consecutive_empty_count = 0
    for index, cell in enumerate(cleaned_df["Column_0"]):
        if pd.isna(cell) or cell == "":
            consecutive_empty_count += 1
            if consecutive_empty_count == 3:
                row_index = index
                rows_to_drop.extend([row_index - 2, row_index - 1, row_index])
                if date is None:
                    for col in cleaned_df.columns:
                        val = cleaned_df.at[row_index, col]
                        if pd.notna(val) and val != "":
                            raw_date = str(val).split(" ")[0]
                            match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_date)
                            if match:
                                month, day, year = match.groups()
                                date_mm = f"{int(month):02d}"
                                date_yyyy = year
                                date = f"{year}-{date_mm}"
                            break
        else:
            consecutive_empty_count = 0

    # ----------------------
    # Additional row drops
    for index, cell in enumerate(cleaned_df["Column_0"]):
        # Drop rows starting with "Total"
        if isinstance(cell, str) and cell.strip().startswith("Total"):
            rows_to_drop.append(index)
        # Drop rows starting with "Name" + next row
        if isinstance(cell, str) and cell.strip().startswith("Name"):
            rows_to_drop.extend([index, index + 1])

    # Remove duplicates, drop them
    rows_to_drop = list(set(rows_to_drop))
    cleaned_df.drop(rows_to_drop, inplace=True)
    cleaned_df.reset_index(drop=True, inplace=True)

    # ----------------------
    # Insert columns 6..14 at the end (extra columns for Commission Date)
    for i in range(6, 15):
        cleaned_df[f"Column_{i}"] = None

    # Fill columns 9..11 with the extracted date for Revenue Recognition
    cleaned_df["Column_9"] = date  # Revenue Recognition Date
    cleaned_df["Column_10"] = date_mm  # Revenue Recognition Date MM
    cleaned_df["Column_11"] = date_yyyy  # Revenue Recognition Date YYYY

    # Leave Commission Date columns empty to be populated by the sales_data_upload.py
    cleaned_df["Column_12"] = ""  # Commission Date
    cleaned_df["Column_13"] = ""  # Commission Date MM 
    cleaned_df["Column_14"] = ""  # Commission Date YYYY

    # ----------------------
    # If Column_1 is populated, fill Column_7 with next row's Column_0
    for index in range(len(cleaned_df) - 1):
        if not pd.isna(cleaned_df.at[index, "Column_1"]) and cleaned_df.at[index, "Column_1"] != "":
            cleaned_df.at[index, "Column_7"] = cleaned_df.at[index + 1, "Column_0"]

    # If Column_7 is populated, last 5 go to Column_8, first 2 remain in Column_7
    for index in range(len(cleaned_df)):
        val = cleaned_df.at[index, "Column_7"]
        if pd.notna(val) and val != "":
            val_str = str(val)
            cleaned_df.at[index, "Column_8"] = val_str[-5:]
            cleaned_df.at[index, "Column_7"] = val_str[:2]

    # ----------------------
    # Fill Column_6 with first row's Column_0 (strip .00)
    if not cleaned_df.empty:
        first_row_value = str(cleaned_df.at[0, "Column_0"]).replace(".00", "")
        cleaned_df["Column_6"] = first_row_value

    # Replace empty strings with NaN in columns 1..5, drop rows that have NaNs
    columns_to_check = ["Column_1", "Column_2", "Column_3", "Column_4", "Column_5"]
    cleaned_df[columns_to_check] = cleaned_df[columns_to_check].replace("", pd.NA)
    cleaned_df.dropna(subset=columns_to_check, inplace=True)
    cleaned_df.reset_index(drop=True, inplace=True)

    # ----------------------
    # Rename columns to final names with updated Revenue Recognition Date fields and Commission Date fields
    cleaned_df.columns = [
        "Client Name",           # Column_0
        "Invoice #",             # Column_1
        "Item ID",               # Column_2
        "Net Sales Amount",      # Column_3
        "Comm Rate",             # Column_4
        "Comm $",                # Column_5
        "Sales Rep Code",        # Column_6
        "State",                 # Column_7
        "ZIP Code",              # Column_8
        "Revenue Recognition Date",      # Column_9
        "Revenue Recognition Date MM",   # Column_10
        "Revenue Recognition Date YYYY", # Column_11
        "Commission Date",              # Column_12
        "Commission Date MM",           # Column_13
        "Commission Date YYYY"          # Column_14
    ]

    # ----------------------
    # Insert new columns after "Comm $"
    cols = cleaned_df.columns.tolist()
    insert_pos = cols.index("Comm $") + 1
    new_cols = ["Sales Rep Name"]
    for nc in reversed(new_cols):
        cols.insert(insert_pos, nc)
    cleaned_df = cleaned_df.reindex(columns=cols)

    # Initialize them
    cleaned_df["Sales Rep Name"] = ""         

    # Convert "Comm $" to numeric (remove commas if any) for calculations
    cleaned_df["Comm $"] = (
        cleaned_df["Comm $"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    cleaned_df["Comm $"] = pd.to_numeric(cleaned_df["Comm $"], errors="coerce")

    # ----------------------
    # Format numeric columns
    columns_to_format = [
        "Net Sales Amount",
        "Comm Rate",
        "Comm %",
    ]
    for col in columns_to_format:
        if col in cleaned_df.columns:
            # Remove commas if any
            cleaned_df[col] = (
                cleaned_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
            )
            # Convert to float
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")
            # Keep them numeric and just round them to 2 decimals:
            cleaned_df[col] = cleaned_df[col].round(2)

    return cleaned_df, date, date_mm, date_yyyy

def load_pdf_file_summit_medical(filepath: str) -> pd.DataFrame:
    """
    Main function to load and process a Summit Medical PDF:
      1) Extract data with Camelot,
      2) Clean & label columns,
      3) Drop unneeded rows & parse date,
      4) Insert new columns after 'Comm $',
      5) Return the final DataFrame.
      
    For PDF files, Revenue Recognition Date fields are populated from the PDF content,
    while Commission Date fields are left empty to be filled by sales_data_upload.py.
    """
    # Step 1: Extract raw data
    raw_data = extract_tables_from_pdf(filepath)
    # Step 2: Clean extracted data
    cleaned_data = clean_extracted_data(raw_data)
    # Step 3: Format table & add calculations
    processed_data, _, _, _ = format_table_logic_and_update_df(cleaned_data)
    return processed_data

def load_excel_file_summit_medical(filepath: str, year: str = None, month: str = None, 
                             rev_year: str = None, rev_month: str = None) -> pd.DataFrame:
    """
    Load and transform a Summit Medical Excel file into a pandas DataFrame.
    
    Args:
        filepath: Path to the Excel file
        year: Selected Commission Date year
        month: Selected Commission Date month
        rev_year: Selected Revenue Recognition year
        rev_month: Selected Revenue Recognition month
        
    Returns:
        Transformed DataFrame with standardized columns
    
    Note:
        Excel column mapping to standardized format:
        - Row Labels -> Client Name
        - Invoice # -> Invoice #
        - Item -> Item ID
        - Sum of Net Sales Amount -> Net Sales Amount
        - CommRate -> Comm Rate
        - Sum of Comm $ -> Comm $
        - St -> State
        - Zip Code -> ZIP Code
    """
    # Check if Commission Date year and month are provided
    if not year or not month:
        raise ValueError("Commission Date year and month must be provided for Summit Medical Excel files")
    
    # Convert month names to numbers (1-12)
    month_map = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    month_num = month_map.get(month)
    if not month_num:
        raise ValueError(f"Invalid Commission Date month: {month}")
    
    # Format Commission Date string
    commission_date = f"{year}-{month_num}"
    
    # Process Revenue Recognition date if provided
    has_revenue_recognition = rev_year is not None and rev_month is not None
    if has_revenue_recognition:
        rev_month_num = month_map.get(rev_month)
        if not rev_month_num:
            raise ValueError(f"Invalid Revenue Recognition month: {rev_month}")
        revenue_recognition_date = f"{rev_year}-{rev_month_num}"
    else:
        # If no revenue recognition date provided, leave it empty
        revenue_recognition_date = ""
        rev_month_num = ""
        rev_year = ""
    
    try:
        # First attempt - try to read with specified dtype for text columns
        df = pd.read_excel(
            filepath, 
            dtype={
                'Invoice #': str,
                'ZIP Code': str
            }
        )
    except Exception:
        # Fallback - read without dtype specification
        df = pd.read_excel(filepath)
        # Then fix the columns manually
        if 'Invoice #' in df.columns:
            df['Invoice #'] = df['Invoice #'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
        if 'ZIP Code' in df.columns:
            df['ZIP Code'] = df['ZIP Code'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
    
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(df, "Summit Medical Excel")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    # Drop rows with empty values in critical columns
    df = df.dropna(subset=["St", "ZIP Code", "Invoice #", "Item", 
                           "CommRate"], 
                  how='any')
    
    # Format columns to match PDF version
    
    # 1. Fix text columns that might have been interpreted as numbers
    # Invoice # as text - handle float conversion if needed
    if "Invoice #" in df.columns:
        df["Invoice #"] = df["Invoice #"].apply(
            lambda x: str(int(float(x))) if pd.notnull(x) and not isinstance(x, str) else str(x)
        ).str.strip()
    
    # ZIP Code as text - handle float conversion if needed
    if "ZIP Code" in df.columns:
        df["ZIP Code"] = df["ZIP Code"].apply(
            lambda x: str(int(float(x))) if pd.notnull(x) and not isinstance(x, str) else str(x)
        ).str.strip()
    
    # State as text
    if "St" in df.columns:
        df["St"] = df["St"].astype(str).str.strip()
    
    # 2. Convert numeric columns
    numeric_columns = ["CommRate", "Sum of Net Sales Amount", "Sum of Comm $"]
    for col in numeric_columns:
        if col in df.columns:
            # Remove any currency symbols or thousand separators
            if df[col].dtype == object:  # If it's already a string
                df[col] = df[col].str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            # Convert to numeric and round to 2 decimals
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    
    # Rename columns to match standardized format
    column_mapping = {
        "Row Labels": "Client Name",
        "Invoice #": "Invoice #",  # Same name, no change needed
        "Item": "Item ID",
        "Sum of Net Sales Amount": "Net Sales Amount",
        "CommRate": "Comm Rate",
        "Sum of Comm $": "Comm $",
        "St": "State",
        "ZIP Code": "ZIP Code"
    }
    df = df.rename(columns=column_mapping)
    
    # Add Revenue Recognition Date columns
    df["Revenue Recognition Date"] = revenue_recognition_date
    df["Revenue Recognition Date MM"] = rev_month_num
    df["Revenue Recognition Date YYYY"] = rev_year
    
    # Add Commission Date columns populated with user-selected values
    df["Commission Date"] = commission_date
    df["Commission Date MM"] = month_num
    df["Commission Date YYYY"] = year
    
    # Add Sales Rep Code column
    df["Sales Rep Code"] = "Streamline Hospital Services, LLC"
    
    # Add Sales Rep Name column (to be enriched later)
    df["Sales Rep Name"] = ""
    
    # Ensure all required columns exist in the output
    expected_columns = ["Client Name", "Invoice #", "Item ID", "Net Sales Amount", 
                        "Comm Rate", "Comm $", "Sales Rep Code", "State", 
                        "ZIP Code", "Revenue Recognition Date", "Revenue Recognition Date MM", "Revenue Recognition Date YYYY", 
                        "Sales Rep Name", "Commission Date", "Commission Date MM", "Commission Date YYYY"]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Final formatting to ensure consistency
    # Client Name as text
    df["Client Name"] = df["Client Name"].astype(str).str.strip()
    # Item ID as text
    df["Item ID"] = df["Item ID"].astype(str).str.strip()

    # Reorder columns to match standard format
    return df[expected_columns]