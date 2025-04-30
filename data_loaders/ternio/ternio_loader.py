import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np
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
        WHERE "Source" = 'Ternio'
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

def load_excel_file_ternio(filepath: str) -> pd.DataFrame:
    """
    Load and transform a Ternio Excel file into a pandas DataFrame.
    
    Processing steps:
      1. Drop the first 4 rows (the header is in row 5)
      2. Drop columns if ALL these columns have empty values: 
         [Col A, Col B, Col C, Col D, Col E]
      3. Use row 5 for header names (colA to colG)
      4. Rename and reformat columns as specified
      5. Apply complex merge logic for Payment and Invoice rows
    """
    # Read the Excel file starting from the 6th row (index 5)
    # First read the header row (row 5) to get column names
    header_df = pd.read_excel(filepath, header=None, skiprows=4, nrows=1)
    
    # Get column headers from the first row
    column_headers = header_df.iloc[0, :7].tolist()
    
    # Ensure column headers are valid
    # The first column is often None but we need a valid column name
    if column_headers[0] is None or pd.isna(column_headers[0]):
        column_headers[0] = "Unnamed"
    
    # Read the actual data, starting from row 6
    # Use dtype parameter to explicitly set Num column as string
    raw_df = pd.read_excel(filepath, header=None, skiprows=5, dtype={4: str})  # Column index 4 is "Num" based on your headers
    
    # Apply the column headers to the first 7 columns
    columns_to_use = min(len(raw_df.columns), 7)
    column_headers = column_headers[:columns_to_use]
    
    # Ensure the DataFrame has at least 7 columns
    for i in range(columns_to_use, 7):
        raw_df[i] = None
        column_headers.append(f"Column_{i}")
    
    # Rename the first 7 columns
    for i in range(7):
        if i < len(raw_df.columns):
            raw_df = raw_df.rename(columns={i: column_headers[i]})
    
    # Drop rows where the first 5 columns (A-E) are all empty
    cols_to_check = column_headers[:5]
    raw_df = raw_df.dropna(subset=cols_to_check, how='all')
    
    # Process the DataFrame as needed
    df = raw_df.copy()
    
    # Ensure 'Date' column is properly formatted (if it exists)
    if "Date" in df.columns:
        # First convert to datetime, handling potential errors
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        
        # Add 'Revenue Recognition Date YYYY' and 'Revenue Recognition Date MM' columns, handling NaN values
        df["Revenue Recognition Date YYYY"] = df["Date"].dt.year.astype("Int64").astype(str)
        # Use dt.month directly without formatting
        month_values = df["Date"].dt.month
        # Convert to string and zero-pad month values, handling NaN
        df["Revenue Recognition Date MM"] = month_values.apply(
            lambda x: f"{int(x):02d}" if pd.notnull(x) else ""
        )
        
        # Format 'Date' as YYYY-MM-DD
        df["Revenue Recognition Date"] = df["Date"].dt.strftime('%Y-%m-%d')
        
        # Drop the original Date column
        df = df.drop(columns=["Date"])
    
    # Ensure "Num" column is treated as a string without any formatting
    if "Num" in df.columns:
        # Remove any commas and convert to plain string format
        df["Num"] = df["Num"].astype(str).str.replace(',', '', regex=False)
        # Handle numeric values by converting them to simple strings without formatting
        df["Num"] = df["Num"].apply(
            lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.', '', 1).isdigit() else str(x)
        )
        # Remove 'nan' strings
        df["Num"] = df["Num"].replace('nan', '')
    
    # Convert numeric columns while preserving Num as a string
    numeric_columns = ["Invoiced", "Paid"]
    for col in numeric_columns:
        if col in df.columns:
            # Convert to string first, then process
            df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            # Convert to numeric, handling any errors
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # === COLUMN REFORMATTING AS REQUESTED ===
    
    # Rename columns
    column_mapping = {
        "Unnamed": "Client Name",
        "Transaction Type": "Transaction Type 1",
        "Date": "Revenue Recognition Date"  # This shouldn't be needed anymore but keeping for safety
    }
    df = df.rename(columns=column_mapping)
    
    # Create new columns
    df["Transaction Type 2"] = ""  # Empty placeholder
    df["Invoice Date"] = ""  # Empty placeholder for date
    df["Product Line"] = "Miscellaneous"  # Fixed value
    df["Comm Rate"] = 0.07  # Fixed value (7%)
    df["Comm Amount"] = df["Paid"] * df["Comm Rate"]  # Calculated
    
    # The Commission Date columns will be populated by sales_data_upload.py
    # Here we're just creating empty placeholders
    df["Commission Date"] = ""
    df["Commission Date YYYY"] = ""
    df["Commission Date MM"] = ""
    
    # Add Sales Rep Name based on lookup
    try:
        # Load master data for Sales Rep lookup
        master_df = load_master_sales_rep()
        
        # Function to lookup Sales Rep Name based on Client Name and dates
        def lookup_sales_rep(row):
            client_name = row["Client Name"]
            payment_date = pd.to_datetime(row["Revenue Recognition Date"]) if pd.notna(row["Revenue Recognition Date"]) else None
            
            if pd.isna(client_name) or client_name == "" or payment_date is None:
                return ""
                
            # Filter master_df for matching Client Names
            matches = master_df[
                (master_df["Customer field"] == "Client Name") & 
                (master_df["Data field value"] == client_name)
            ]
            
            # Filter by date validity
            valid_matches = matches[
                (matches["Valid from"] <= payment_date) & 
                ((matches["Valid until"].isna()) | (matches["Valid until"] >= payment_date))
            ]
            
            if not valid_matches.empty:
                return valid_matches.iloc[0]["Sales Rep name"]
            return ""
        
        # Apply the lookup function to each row
        df["Sales Rep Name"] = df.apply(lookup_sales_rep, axis=1)
    except Exception as e:
        print(f"Error enriching Sales Rep Name: {e}")
        df["Sales Rep Name"] = ""  # Fallback if lookup fails
    
    # Round numeric values to 2 decimal places
    df["Invoiced"] = df["Invoiced"].round(2)
    df["Paid"] = df["Paid"].round(2)
    df["Comm Amount"] = df["Comm Amount"].round(2)
    df["Comm Rate"] = df["Comm Rate"].round(2)
    
    # ==========================================
    # Complex merge logic for Payment and Invoice rows
    # ==========================================
    
    # Make a copy to avoid issues with modifying during iteration
    merged_df = df.copy()
    
    # Reset index to make it easier to work with
    merged_df = merged_df.reset_index(drop=True)
    
    # List to track indices to drop
    to_drop = []
    
    # Step 1-4: Process each row in the DataFrame
    i = 0
    while i < len(merged_df):
        # If Client Name is populated (Case 1)
        if pd.notna(merged_df.loc[i, "Client Name"]) and merged_df.loc[i, "Client Name"] != "":
            # Check if we have at least two more rows
            if i+2 < len(merged_df):
                # Check if the pattern matches: Current row followed by Payment then Invoice
                if (merged_df.loc[i+1, "Transaction Type 1"] == "Payment" and 
                    merged_df.loc[i+2, "Transaction Type 1"] == "Invoice"):
                    
                    # Copy data from Payment row (i+1) to current row (i)
                    merged_df.loc[i, "Transaction Type 1"] = merged_df.loc[i+1, "Transaction Type 1"]
                    merged_df.loc[i, "Revenue Recognition Date"] = merged_df.loc[i+1, "Revenue Recognition Date"]
                    merged_df.loc[i, "Revenue Recognition Date YYYY"] = merged_df.loc[i+1, "Revenue Recognition Date YYYY"]
                    merged_df.loc[i, "Revenue Recognition Date MM"] = merged_df.loc[i+1, "Revenue Recognition Date MM"]
                    merged_df.loc[i, "Paid"] = merged_df.loc[i+1, "Paid"]
                    merged_df.loc[i, "Comm Amount"] = merged_df.loc[i+1, "Comm Amount"]
                    
                    # Copy data from Invoice row (i+2) to current row (i)
                    merged_df.loc[i, "Transaction Type 2"] = merged_df.loc[i+2, "Transaction Type 1"]
                    merged_df.loc[i, "Invoice Date"] = merged_df.loc[i+2, "Revenue Recognition Date"]
                    merged_df.loc[i, "Memo/Description"] = merged_df.loc[i+2, "Memo/Description"]
                    merged_df.loc[i, "Num"] = merged_df.loc[i+2, "Num"]
                    merged_df.loc[i, "Invoiced"] = merged_df.loc[i+2, "Invoiced"]
                    
                    # Mark rows i+1 and i+2 for deletion
                    to_drop.extend([i+1, i+2])
                    
                    # Skip the rows we just processed
                    i += 3
                    continue
            
            # Check if this is the last row with a populated Client Name
            if i == len(merged_df) - 1 or i + 1 >= len(merged_df):
                to_drop.append(i)
                i += 1
                continue
                
        # Case 3: Empty Client Name and Transaction Type 1 is "Payment"
        elif ((pd.isna(merged_df.loc[i, "Client Name"]) or merged_df.loc[i, "Client Name"] == "") and
              merged_df.loc[i, "Transaction Type 1"] == "Payment"):
            
            # Check if we have at least one more row and there's a previous row
            if i > 0 and i+1 < len(merged_df):
                # Find the most recent populated Client Name
                prev_idx = i - 1
                while prev_idx >= 0:
                    if (pd.notna(merged_df.loc[prev_idx, "Client Name"]) and 
                        merged_df.loc[prev_idx, "Client Name"] != ""):
                        # Copy Client Name from previous populated row
                        merged_df.loc[i, "Client Name"] = merged_df.loc[prev_idx, "Client Name"]
                        break
                    prev_idx -= 1
                
                # Copy data from next row (Invoice)
                merged_df.loc[i, "Transaction Type 2"] = merged_df.loc[i+1, "Transaction Type 1"]
                merged_df.loc[i, "Invoice Date"] = merged_df.loc[i+1, "Revenue Recognition Date"]
                merged_df.loc[i, "Memo/Description"] = merged_df.loc[i+1, "Memo/Description"]
                merged_df.loc[i, "Num"] = merged_df.loc[i+1, "Num"]
                merged_df.loc[i, "Invoiced"] = merged_df.loc[i+1, "Invoiced"]
                
                # Mark row i+1 for deletion
                to_drop.append(i+1)
                
                # Skip the row we just processed
                i += 2
                continue
        
        # Move to the next row
        i += 1
    
    # Drop the rows marked for deletion
    merged_df = merged_df.drop(to_drop).reset_index(drop=True)
    
    # Reorder columns in the specified sequence
    ordered_columns = [ 
        "Client Name",
        "Commission Date",
        "Commission Date YYYY",
        "Commission Date MM",
        "Revenue Recognition Date",
        "Revenue Recognition Date YYYY",
        "Revenue Recognition Date MM",
        "Invoice Date",
        "Memo/Description",
        "Sales Rep Name",
        "Product Line",
        "Num",
        "Invoiced",
        "Paid",
        "Comm Rate",
        "Comm Amount"
    ]
    
    # Only include columns that exist in the DataFrame
    existing_columns = [col for col in ordered_columns if col in merged_df.columns]
    merged_df = merged_df[existing_columns]
    
    # Re-enrich Sales Rep Name after merging rows
    try:
        # Load master data for Sales Rep lookup
        master_df = load_master_sales_rep()
        
        # Function to lookup Sales Rep Name based on Client Name and dates
        def lookup_sales_rep(row):
            client_name = row["Client Name"]
            payment_date = pd.to_datetime(row["Revenue Recognition Date"]) if pd.notna(row["Revenue Recognition Date"]) else None
            
            if pd.isna(client_name) or client_name == "" or payment_date is None:
                return ""
                
            # Filter master_df for matching Client Names
            matches = master_df[
                (master_df["Customer field"] == "Client Name") & 
                (master_df["Data field value"] == client_name)
            ]
            
            # Filter by date validity
            valid_matches = matches[
                (matches["Valid from"] <= payment_date) & 
                ((matches["Valid until"].isna()) | (matches["Valid until"] >= payment_date))
            ]
            
            if not valid_matches.empty:
                return valid_matches.iloc[0]["Sales Rep name"]
            return ""
        
        # Apply the lookup function to each row
        merged_df["Sales Rep Name"] = merged_df.apply(lookup_sales_rep, axis=1)
    except Exception as e:
        print(f"Error enriching Sales Rep Name after merge: {e}")
    
    # Run validation on the processed DataFrame
    is_valid, missing = validate_file_format(merged_df, "Ternio")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    return merged_df