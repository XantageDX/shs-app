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
        year: Selected year (required)
        month: Selected month (required)
        
    Returns:
        Transformed DataFrame with standardized columns
    """
    # Check if year and month are provided
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
    
    # Format date string
    date_str = f"{year}-{month_num}"
    
    # Read Excel file with converters to preserve the original format of certain columns
    # This approach reads Customer PO Number as strings to preserve scientific notation
    converters = {
        'Customer PO Number': str,
        'Ship To Zip Code': str,
        'Invoice Number': str,
        'Customer Number': str,
        'Sales Order Number': str
    }
    
    raw_df = pd.read_excel(filepath, header=2, converters=converters)
    
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Novo")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")
    
    # Proceed with the cleaning and transformation
    df = raw_df.copy()
    
    # Format Invoice Date from MM-DD-YYYY to YYYY-MM-DD
    if "Invoice Date" in df.columns:
        df["Invoice Date"] = pd.to_datetime(df["Invoice Date"], errors='coerce').dt.strftime('%Y-%m-%d')

    # Format Order Date to YYYY-MM-DD string format
    if "Order Date" in df.columns:
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Add the date columns based on user selection
    df["Commission Date"] = date_str
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
                                temp_series[idx] = round(numeric_value, 2)
                            else:
                                # Otherwise assume it's a percentage value (e.g., 5 for 5%)
                                temp_series[idx] = round(numeric_value / 100, 2)
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
    
    # Reorder columns to ensure consistent format
    all_expected_columns = [
        "Salesperson Number", "AR Division Number", "Customer Number", 
        "Bill To Name", "Ship To Name", "Invoice Number", "Invoice Date",
        "Customer PO Number", "Item Code", "Alias Item Number", 
        "Item Code Description", "Commission Date", "Commission Date YYYY", "Commission Date MM",
        "Quantity Ordered", "Qty Shipped", "Quantity Backordered",
        "Unit Price", "Extension", "Comment", "Order Date", 
        "Sales Order Number", "Ship To State", "Ship To Zip Code",
        "UD F LOTBUS", "Sales Rep Name", "Commission Percentage", "Commission Amount"
    ]
    
    # Only keep columns that exist in the DataFrame
    existing_columns = [col for col in all_expected_columns if col in df.columns]
    df = df[existing_columns]
    
    return df