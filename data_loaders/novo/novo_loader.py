import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import re
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
        WHERE "Source" = 'NOVO DIRECT'
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

def load_excel_file_novo(filepath: str, year: str = None, month: str = None) -> pd.DataFrame:
    """
    Load and transform a Novo Excel file into a pandas DataFrame.
    
    Args:
        filepath: Path to the Excel file
        year: Selected year (required) for Commission Date
        month: Selected month (required) for Commission Date
        
    Returns:
        Transformed DataFrame with standardized columns
    """
    # Check if year and month are provided for Commission Date
    if not year or not month:
        raise ValueError("Year and month must be provided for Novo Excel files")
    
    # Convert month name to number (1-12)
    month_map = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    month_num = month_map.get(month)
    if not month_num:
        raise ValueError(f"Invalid month: {month}")
    
    # Format Commission Date string
    commission_date_str = f"{year}-{month_num}"
    
    # Read Excel file with converters to preserve the original format of certain columns
    # This approach reads Customer PO Number as strings to preserve scientific notation
    converters = {
        'Customer PO Number': str,
        'Ship To Zip Code': str,
        'Invoice Number': str,
        'Customer Number': str,
        'Sales Order Number': str
    }
    
    # Skip the first column (colA) which is empty
    raw_df = pd.read_excel(filepath, header=2, converters=converters, usecols=lambda x: x != "Unnamed: 0")
    
    # If Unnamed:0 still appears, explicitly drop it
    if "Unnamed: 0" in raw_df.columns:
        raw_df = raw_df.drop(columns=["Unnamed: 0"])
    
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Novo")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    # Proceed with the cleaning and transformation
    df = raw_df.copy()
    
    # Handle "Invoice Date" column - now becomes "Revenue Recognition Date"
    if "Invoice Date" in df.columns:
        # Format "Invoice Date" to "YYYY-MM-DD" string format if it's not already
        df["Revenue Recognition Date"] = pd.to_datetime(df["Invoice Date"], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Extract "Revenue Recognition Date YYYY" - first 4 characters of the date string
        df["Revenue Recognition Date YYYY"] = df["Revenue Recognition Date"].str[:4]
        
        # Extract "Revenue Recognition Date MM" - characters between the first and second hyphen
        df["Revenue Recognition Date MM"] = df["Revenue Recognition Date"].str[5:7]
        
        # Drop the original "Invoice Date" column
        df = df.drop(columns=["Invoice Date"])

    # Format Order Date to "YYYY-MM-DD" string format
    if "Order Date" in df.columns:
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Add the Commission Date columns based on user selection
    df["Commission Date"] = commission_date_str
    df["Commission Date YYYY"] = year
    df["Commission Date MM"] = month_num

    # Preserve the original format of Customer PO Number
    if "Customer PO Number" in df.columns:
        # Simply ensure it's a string but don't convert scientific notation to full numbers
        df["Customer PO Number"] = df["Customer PO Number"].apply(
            lambda x: "" if pd.isna(x) else str(x)
        )
    
    text_columns = [
        "Customer Number", "Invoice Number", "Sales Order Number", 
        "Ship To Zip Code", "Salesperson Number", "AR Division Number"
    ]
    
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str(x) if pd.notnull(x) else ""
            )
    
    # Convert numeric columns to float with 2 decimal places
    numeric_columns = [
        "Quantity Ordered", "Qty Shipped", "Quantity Backordered", 
        "Unit Price", "Extension", "Commission Percentage", "Commission Amount"
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            # Special handling for Commission Percentage
            if col == "Commission Percentage":
                # Create a temporary series to handle percentage values
                temp_series = pd.Series(index=df.index)
                
                for idx, value in df[col].items():
                    if pd.isna(value):
                        temp_series[idx] = 0
                    elif isinstance(value, str) and '%' in value:
                        # If it's already in percentage format with % sign
                        try:
                            # Remove % sign and convert to decimal
                            cleaned_value = value.replace('%', '').strip().replace(',', '.')
                            temp_series[idx] = float(cleaned_value) / 100
                        except (ValueError, TypeError):
                            temp_series[idx] = 0
                    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                        # If it's a numeric value
                        try:
                            numeric_value = float(value)
                            # If the value is too large (likely already divided), use it directly
                            if numeric_value < 1:
                                temp_series[idx] = round(numeric_value, 4)
                            else:
                                # Otherwise assume it's a percentage value (e.g., 5 for 5%)
                                temp_series[idx] = round(numeric_value / 100, 4)
                        except (ValueError, TypeError):
                            temp_series[idx] = 0
                    else:
                        temp_series[idx] = 0
                
                # Replace the original column with our fixed values
                df[col] = temp_series
            else:
                # For other numeric columns
                # Remove $ and commas from monetary values
                if df[col].dtype == object:  # If it's a string/object column
                    df[col] = df[col].apply(
                        lambda x: x.replace('$', '').replace(',', '') if isinstance(x, str) else x
                    )
                
                # Convert to numeric and round to 2 decimal places
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
    
    # Add Sales Rep Name based on Customer Number lookup
    master_df = load_master_sales_rep()
    
    def lookup_sales_rep(customer_number):
        """Look up the Sales Rep Name for a given Customer Number."""
        if pd.isna(customer_number) or customer_number == "":
            return ""
            
        # Find matches in master_sales_rep where Customer field is 'Customer Number'
        # and Data field value matches the customer_number
        matches = master_df[
            (master_df["Customer field"] == "Customer Number") & 
            (master_df["Data field value"] == str(customer_number))
        ]
        
        if not matches.empty:
            return matches.iloc[0]["Sales Rep name"]
        
        return ""
    
    if "Customer Number" in df.columns:
        df["Sales Rep Name"] = df["Customer Number"].apply(lookup_sales_rep)
    else:
        df["Sales Rep Name"] = ""

    # Move "Sales Rep Name" column to after "UD F LOTBUS" 
    if "Sales Rep Name" in df.columns and "UD F LOTBUS" in df.columns:
        # Get all column names
        all_columns = list(df.columns)
        
        # Remove "Sales Rep Name" from its current position
        all_columns.remove("Sales Rep Name")
        
        # Find the position of "UD F LOTBUS"
        ud_f_lotbus_index = all_columns.index("UD F LOTBUS")
        
        # Insert "Sales Rep Name" after "UD F LOTBUS"
        all_columns.insert(ud_f_lotbus_index + 1, "Sales Rep Name")
        
        # Reorder the DataFrame columns
        df = df[all_columns]

    # Reorder columns to group date fields together
    # First, get all columns except the date columns we want to reorder
    date_columns = ["Revenue Recognition Date", "Revenue Recognition Date YYYY", "Revenue Recognition Date MM", 
                   "Commission Date", "Commission Date YYYY", "Commission Date MM"]
    non_date_columns = [col for col in df.columns if col not in date_columns]
    
    # Find the position to insert Revenue Recognition Date columns (after "Customer PO Number")
    if "Customer PO Number" in non_date_columns:
        po_index = non_date_columns.index("Customer PO Number")
        # Insert Revenue Recognition Date columns
        rev_date_columns = ["Revenue Recognition Date", "Revenue Recognition Date YYYY", "Revenue Recognition Date MM"]
        for i, col in enumerate(rev_date_columns):
            if col in df.columns:
                non_date_columns.insert(po_index + 1 + i, col)
    
    # Find the position to insert Commission Date columns (after Item Code Description)
    if "Item Code Description" in non_date_columns:
        desc_index = non_date_columns.index("Item Code Description")
        # Insert Commission Date columns
        comm_date_columns = ["Commission Date", "Commission Date YYYY", "Commission Date MM"]
        for i, col in enumerate(comm_date_columns):
            if col in df.columns:
                non_date_columns.insert(desc_index + 1 + i, col)
    
    # Ensure all columns from the DataFrame are included in the final order
    final_columns = []
    for col in non_date_columns:
        if col in df.columns and col not in final_columns:
            final_columns.append(col)
    
    # Reorder the DataFrame columns
    df = df[final_columns]
    
    return df