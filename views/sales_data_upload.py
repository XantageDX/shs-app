import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os
import tempfile
import datetime

# Import all the loaders
from data_loaders.cygnus.cygnus_loader import load_excel_file_cygnus
from data_loaders.logiquip.logiquip_loader import load_excel_file_logiquip
from data_loaders.summit_medical.summit_medical_loader import load_pdf_file_summit_medical, load_excel_file_summit_medical
from data_loaders.quickbooks.quickbooks_loader import load_excel_file_quickbooks
from data_loaders.inspektor.inspektor_loader import load_excel_file_inspektor
from data_loaders.sunoptic.sunoptic_loader import load_excel_file_sunoptic
from data_loaders.ternio.ternio_loader import load_excel_file_ternio
from data_loaders.novo.novo_loader import load_excel_file_novo
from data_loaders.chemence.chemence_loader import load_excel_file_chemence

# Import all the DB utils
from data_loaders.cygnus.cygnus_db_utils import save_dataframe_to_db as save_cygnus_to_db
from data_loaders.logiquip.logiquip_db_utils import save_dataframe_to_db as save_logiquip_to_db
from data_loaders.summit_medical.summit_medical_db_utils import save_dataframe_to_db as save_summit_medical_to_db
from data_loaders.quickbooks.quickbooks_db_utils import save_dataframe_to_db as save_quickbooks_to_db
from data_loaders.inspektor.inspektor_db_utils import save_dataframe_to_db as save_inspektor_to_db
from data_loaders.sunoptic.sunoptic_db_utils import save_dataframe_to_db as save_sunoptic_to_db
from data_loaders.ternio.ternio_db_utils import save_dataframe_to_db as save_ternio_to_db
from data_loaders.novo.novo_db_utils import save_dataframe_to_db as save_novo_to_db
from data_loaders.chemence.chemence_db_utils import save_dataframe_to_db as save_chemence_to_db

# Import validation_utils
from data_loaders.validation_utils import validate_file_format, EXPECTED_COLUMNS

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

# Define file types and their corresponding handlers
FILE_TYPES = {
    "Logiquip": "Logiquip",
    "Cygnus": "Cygnus",
    "Summit Medical": "Summit Medical",
    "QuickBooks": "QuickBooks",
    "InspeKtor": "InspeKtor",
    "Sunoptic": "Sunoptic",
    "Ternio": "Ternio",
    "Novo": "Novo",
    "Chemence": "Chemence",
}

# Dictionary specifying if a file type's loader already handles commission dates internally
# This will be used to skip date column addition for loaders that already handle it
LOADERS_WITH_DATE_HANDLING = {
    # File types that already fully handle commission dates in their loaders
    "Chemence": False,  
    # File types that need us to add Commission Date columns
    "Cygnus": False,
    "Logiquip": False,
    "Summit Medical": False,
    "QuickBooks": False,
    "InspeKtor": False,
    "Sunoptic": False,
    "Ternio": False,
    "Novo": False
}

def load_file(filepath: str, file_type: str, debug_info: list, year: str = None, month: str = None) -> pd.DataFrame:
    """
    Generic dispatcher to the correct loader function, with standardized parameters.
    All loaders receive year and month where applicable.
    """
    if file_type == "Cygnus":
        return load_excel_file_cygnus(filepath)
    elif file_type == "Logiquip":
        return load_excel_file_logiquip(filepath)
    elif file_type == "Summit Medical":
        # For Summit Medical, check if the file is PDF or Excel
        if filepath.lower().endswith('.xlsx') or filepath.lower().endswith('.xls'):
            # Check for Revenue Recognition date selections
            rev_year = None
            rev_month = None
            if "summit_rev_selected_year" in st.session_state and "summit_rev_selected_month" in st.session_state:
                rev_year = str(st.session_state["summit_rev_selected_year"])
                rev_month = st.session_state["summit_rev_selected_month"]
            
            return load_excel_file_summit_medical(
                filepath, 
                year=year,
                month=month,
                rev_year=rev_year,
                rev_month=rev_month
            )
        else:
            # For PDF files, use the PDF loader
            return load_pdf_file_summit_medical(filepath)
    elif file_type == "QuickBooks":
        return load_excel_file_quickbooks(filepath)
    elif file_type == "InspeKtor":
        return load_excel_file_inspektor(filepath)
    elif file_type == "Sunoptic":
        return load_excel_file_sunoptic(filepath)
    elif file_type == "Ternio":
        return load_excel_file_ternio(filepath)
    elif file_type == "Novo":
        return load_excel_file_novo(filepath, year=year, month=month)
    elif file_type == "Chemence":
        return load_excel_file_chemence(filepath)
    else:
        return pd.read_excel(filepath)

def check_for_blanks_with_details(df: pd.DataFrame, file_type: str = None) -> list:
    """
    Return a list of (row_number, [columns_with_blanks]) for any row that has blank cells.
    For certain file types, only checks specific columns.
    """
    blank_details = []
    
    # Special case for Novo file type - only check specific columns
    if file_type == "Novo":
        columns_to_check = [
            "Customer Number",
            "Qty Shipped",
            "Unit Price", 
            "Extension",
            "Commission Percentage",
            "Commission Amount",
            "Commission Date",
            "Commission Date YYYY",
            "Commission Date MM"
        ]
        existing_columns = [col for col in columns_to_check if col in df.columns]
        
        for row_idx, row in df.iterrows():
            blank_columns = row[existing_columns][row[existing_columns].isnull() | (row[existing_columns] == "")].index.tolist()
            if blank_columns:
                blank_details.append((row_idx, blank_columns))
    
    # Special case for Chemence file type - only check specific columns
    elif file_type == "Chemence":
        columns_to_check = [
            "Source",
            "Commission Date",
            "Commission Date YYYY",
            "Commission Date MM",
            "Sales Group",
            "Source ID",
            "Sales Rep Name",
            "Account Number",
            "Account Name",
            "Street",
            "City",
            "State",
            "Zip",
            "Description",
            "Part #",
            "Qty Shipped",
            "UOM",
            "Sales Price",
            "Sales Total",
            "Commission",
            "Unit Price"
        ]
        existing_columns = [col for col in columns_to_check if col in df.columns]
        
        for row_idx, row in df.iterrows():
            blank_columns = row[existing_columns][row[existing_columns].isnull() | (row[existing_columns] == "")].index.tolist()
            if blank_columns:
                blank_details.append((row_idx, blank_columns))
    else:
        # Original behavior for all other file types
        for row_idx, row in df.iterrows():
            blank_columns = row[row.isnull() | (row == "")].index.tolist()
            if blank_columns:
                blank_details.append((row_idx, blank_columns))
                
    return blank_details

def check_for_amount_line_issues(df: pd.DataFrame) -> list:
    """
    Check for rows where 'Amount line' is ≤ 0 and return a list of problematic rows.
    """
    issues = []
    if 'Amount line' in df.columns:
        for row_idx, row in df.iterrows():
            try:
                amount = float(row['Amount line'])
                if amount <= 0:
                    issues.append(row_idx + 1)  # Add 1 to row index to make it human-readable
            except (ValueError, TypeError):
                # If we can't convert to float, skip this row
                pass
    return issues

def fetch_table_data(table_name: str) -> pd.DataFrame:
    """Fetch data from a PostgreSQL table."""
    query = f"SELECT * FROM {table_name};"
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def update_table_data(table_name: str, df: pd.DataFrame):
    """Update a PostgreSQL table with the provided DataFrame."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"DELETE FROM {table_name};"))
                df.to_sql(table_name, con=engine, if_exists="append", index=False)
        st.success(f"Changes successfully saved to the {table_name} table!")
    except Exception as e:
        st.error(f"Error updating the {table_name} table: {e}")
    finally:
        engine.dispose()

def get_unique_sales_rep_names():
    """Fetch distinct Sales Rep Names from the sales_rep_commission_tier table."""
    query = """
        SELECT DISTINCT "Sales Rep Name"
        FROM sales_rep_commission_tier
        ORDER BY "Sales Rep Name"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            sales_reps = [row[0] for row in result.fetchall()]
        return sales_reps
    except Exception as e:
        st.error(f"Error fetching unique Sales Rep names: {e}")
        return []
    finally:
        engine.dispose()

def check_for_valid_sales_rep(df: pd.DataFrame) -> list:
    """
    Check if the names in the raw DataFrame column "Sales Rep Name" are present
    in the sales_rep_commission_tier table.
    Returns a list of names that are missing.
    """
    if "Sales Rep Name" not in df.columns:
        return []  # No validation needed if column doesn't exist
    
    # Get the set of valid names from the database
    valid_names = set(get_unique_sales_rep_names())
    
    # Get unique non-empty sales rep names from the dataframe
    df_names = set(df["Sales Rep Name"].dropna().astype(str))
    
    # Remove empty strings from the set of names to check
    df_names = {name for name in df_names if name.strip() != ""}
    
    # Find names that exist in the dataframe but not in the valid names list
    missing_names = list(df_names - valid_names)
    
    return missing_names


def add_commission_date_columns(df: pd.DataFrame, year: str, month: str, month_num: int) -> pd.DataFrame:
    """
    Add standardized Commission Date columns to a DataFrame.
    For Summit Medical files, preserves existing Revenue Recognition Date values.
    
    Returns the updated DataFrame with the following columns added or updated:
    - Commission Date YYYY
    - Commission Date MM
    - Commission Date
    """
    # Check if this is a Summit Medical file by looking for specific columns
    is_summit_medical = (
        "Revenue Recognition Date" in df.columns and 
        "Revenue Recognition Date YYYY" in df.columns and 
        "Revenue Recognition Date MM" in df.columns
    )
    
    # Format the Commission Date columns properly
    df["Commission Date YYYY"] = str(year)
    df["Commission Date MM"] = f"{month_num:02d}"
    
    # Create the Commission Date string directly with the formatted year and month
    df["Commission Date"] = f"{year}-{month_num:02d}"
    
    return df

def check_for_existing_data(df, file_type):
    """
    Check if there is existing data in the database for the Revenue Recognition dates in the dataframe.
    Returns a tuple (has_existing_data, date_info) where:
    - has_existing_data is a boolean indicating if data exists
    - date_info is a string describing the dates with existing data
    """
    # Determine the table name based on file type
    table_names = {
        "Cygnus": "master_cygnus_sales",
        "Logiquip": "master_logiquip_sales",
        "Summit Medical": "master_summit_medical_sales",
        "QuickBooks": "master_quickbooks_sales",
        "InspeKtor": "master_inspektor_sales",
        "Sunoptic": "master_sunoptic_sales",
        "Ternio": "master_ternio_sales",
        "Novo": "master_novo_sales",
        "Chemence": "master_chemence_sales"
    }
    
    table_name = table_names.get(file_type)
    if not table_name:
        return False, ""
    
    # Check which column naming convention is used in this DataFrame for Revenue Recognition Date
    year_col = None
    month_col = None
    
    # Check for different variations of Revenue Recognition column names
    possible_year_cols = ["Revenue Recognition Date YYYY", "Revenue Recognition YYYY"]
    possible_month_cols = ["Revenue Recognition Date MM", "Revenue Recognition MM"]
    
    for col in possible_year_cols:
        if col in df.columns:
            year_col = col
            break
    
    for col in possible_month_cols:
        if col in df.columns:
            month_col = col
            break
    
    if not year_col or not month_col:
        st.warning(f"Could not find Revenue Recognition year/month columns in {file_type} data. Skipping overwrite check.")
        return False, ""
    
    # Get unique combinations of Revenue Recognition year and month
    date_values = df[[year_col, month_col]].drop_duplicates().values.tolist()
    if not date_values:
        return False, ""
    
    # Check the database for existing data with these dates
    engine = get_db_connection()
    existing_periods = []
    
    try:
        with engine.connect() as conn:
            # First, check what column names are used in this table
            inspector = inspect(engine)
            columns = [c["name"] for c in inspector.get_columns(table_name)]
            
            # Determine table column names based on what exists
            table_year_col = None
            table_month_col = None
            
            for col in columns:
                if col in possible_year_cols or col.lower() in [c.lower() for c in possible_year_cols]:
                    table_year_col = col
                if col in possible_month_cols or col.lower() in [c.lower() for c in possible_month_cols]:
                    table_month_col = col
            
            if not table_year_col or not table_month_col:
                st.warning(f"Could not find Revenue Recognition year/month columns in {table_name} table. Skipping overwrite check.")
                return False, ""
            
            # Now check each date combination
            for yyyy, mm in date_values:
                # Convert month number to month name for user-friendly display
                month_name = {
                    "01": "January", "02": "February", "03": "March", "04": "April",
                    "05": "May", "06": "June", "07": "July", "08": "August",
                    "09": "September", "10": "October", "11": "November", "12": "December",
                    1: "January", 2: "February", 3: "March", 4: "April",
                    5: "May", 6: "June", 7: "July", 8: "August",
                    9: "September", 10: "October", 11: "November", 12: "December"
                }.get(mm, mm)
                
                # Check if data exists for this period
                query = text(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE "{table_year_col}" = :yyyy
                    AND "{table_month_col}" = :mm
                """)
                result = conn.execute(query, {"yyyy": str(yyyy), "mm": str(mm)})
                count = result.scalar()
                
                if count > 0:
                    existing_periods.append(f"{month_name} {yyyy} ({count} records)")
    except Exception as e:
        st.error(f"Error checking for existing data: {e}")
        # Print detailed debug info to help troubleshoot
        st.error(f"Debug info: file_type={file_type}, table={table_name}, year_col={year_col}, month_col={month_col}")
        return False, ""
    finally:
        engine.dispose()
    
    if existing_periods:
        return True, ", ".join(existing_periods)
    
    return False, ""

def sales_data_tab():
    st.title("Sales Data Upload Hub")
    
    # Import datetime at the beginning of the function to ensure it's available throughout
    import datetime

    # Initialize a place to store processed dataframes if not present
    if "dataframes" not in st.session_state:
        st.session_state.dataframes = {}

    # Create flags in session state if they don't exist
    if "showing_overwrite_warning" not in st.session_state:
        st.session_state.showing_overwrite_warning = False
    
    if "overwrite_messages" not in st.session_state:
        st.session_state.overwrite_messages = []

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Step 1: Select Product Line")
        # Reset button to clear all session state related to file uploads
        if st.button("Upload a New File"):
            keys_to_clear = [
                "selected_file_type",
                "confirmed_file_bytes",
                "confirmed_file_name",
                "confirmed_file_type",
                "dataframes",
                "selected_year",
                "selected_month",
                "showing_overwrite_warning",
                "overwrite_messages"
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
            
        # Select product line
        selected_file_type = st.selectbox(
            "Choose the product line:",
            list(FILE_TYPES.keys()),
            help="Select the product line you want to process."
        )
        
        # If product line changes, clear previous file data
        if st.session_state.get("selected_file_type") != selected_file_type:
            st.session_state["selected_file_type"] = selected_file_type
            st.session_state.pop("confirmed_file_bytes", None)
            st.session_state.pop("confirmed_file_name", None)
            st.session_state.pop("confirmed_file_type", None)
            
            # Clear date selection when product line changes
            if "selected_year" in st.session_state:
                del st.session_state["selected_year"]
            if "selected_month" in st.session_state:
                del st.session_state["selected_month"]
        
        # Commission Date selection - now standardized for ALL file types
        st.subheader("Commission Date Selection")
        
        # Setup year and month options
        current_year = datetime.datetime.now().year
        year_options = [None, current_year - 1, current_year, current_year + 1]
        month_options = [None, "January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"]
        
        # Year selector
        selected_year = st.selectbox(
            "Select Commission Date Year:", 
            year_options,
            format_func=lambda x: "Select a year..." if x is None else x
        )
        
        # Month selector
        selected_month = st.selectbox(
            "Select Commission Date Month:", 
            month_options,
            format_func=lambda x: "Select a month..." if x is None else x
        )
        
        # Store selections in session state if both are selected
        if selected_year and selected_month:
            st.session_state["selected_year"] = selected_year
            st.session_state["selected_month"] = selected_month
            st.session_state["selected_month_num"] = month_options.index(selected_month)
            st.success(f"Commission Date set to {selected_month}, {selected_year}")
        else:
            # Clear the selection if either is missing
            if "selected_year" in st.session_state:
                del st.session_state["selected_year"]
            if "selected_month" in st.session_state:
                del st.session_state["selected_month"]
            if "selected_month_num" in st.session_state:
                del st.session_state["selected_month_num"]
        
    with col2:
        st.subheader("Step 2: Upload a File to Process")
        uploaded_file = st.file_uploader("Upload a .xlsx or .pdf file:", type=["xlsx", "pdf"])
        if uploaded_file and st.button("Confirm File Selection"):
            file_bytes = uploaded_file.read()
            st.session_state["confirmed_file_bytes"] = file_bytes
            st.session_state["confirmed_file_name"] = uploaded_file.name
            st.session_state["confirmed_file_type"] = uploaded_file.type
            st.success(f"File '{uploaded_file.name}' has been confirmed!")

    # Check if we should prevent processing
    should_stop_processing = False
    
    # Special handling for Summit Medical Excel files to also ask for Revenue Recognition date
    is_summit_excel = (
        st.session_state.get("selected_file_type") == "Summit Medical" and
        "confirmed_file_name" in st.session_state and
        st.session_state["confirmed_file_name"].lower().endswith(('.xlsx', '.xls'))
    )

    if is_summit_excel:
        st.subheader("Revenue Recognition Date Selection")
        st.write("For Summit Medical Excel files, please also select the revenue recognition date:")
        
        # Year and month options for Revenue Recognition Date
        current_year = datetime.datetime.now().year
        year_options = [None, current_year - 1, current_year, current_year + 1]
        month_options = [None, "January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"]
        
        col_rev1, col_rev2 = st.columns(2)
        with col_rev1:
            rev_selected_year = st.selectbox("Select Revenue Recognition Year:", year_options, 
                                    format_func=lambda x: "Select a year..." if x is None else x,
                                    key="summit_rev_year_selector")
        with col_rev2:
            rev_selected_month = st.selectbox("Select Revenue Recognition Month:", month_options,
                                        format_func=lambda x: "Select a month..." if x is None else x,
                                        key="summit_rev_month_selector")
        
        # Check if both selections are made
        if rev_selected_year is not None and rev_selected_month is not None:
            # Store these in session state for later use
            st.session_state["summit_rev_selected_year"] = rev_selected_year
            st.session_state["summit_rev_selected_month"] = rev_selected_month
            st.session_state["summit_rev_selected_month_num"] = month_options.index(rev_selected_month)
            st.success("Revenue Recognition date selected successfully!")
        else:
            # Clear previous selections if either is not selected
            if "summit_rev_selected_year" in st.session_state:
                del st.session_state["summit_rev_selected_year"]
            if "summit_rev_selected_month" in st.session_state:
                del st.session_state["summit_rev_selected_month"]
            if "summit_rev_selected_month_num" in st.session_state:
                del st.session_state["summit_rev_selected_month_num"]

    # Check for missing Revenue Recognition date for Summit Medical Excel files
    if is_summit_excel and (
        "selected_year" not in st.session_state or 
        "selected_month" not in st.session_state or
        "summit_rev_selected_year" not in st.session_state or 
        "summit_rev_selected_month" not in st.session_state
    ):
        # Check if Commission Date is missing
        if "selected_year" not in st.session_state or "selected_month" not in st.session_state:
            st.warning("⚠️ Please select both a year and month for the Commission Date before proceeding.")
            should_stop_processing = True
        # Check if Revenue Recognition Date is missing
        elif "summit_rev_selected_year" not in st.session_state or "summit_rev_selected_month" not in st.session_state:
            st.warning("⚠️ Please select both a year and month for the Revenue Recognition Date before proceeding.")
            should_stop_processing = True

    # Check for date selection
    if "confirmed_file_bytes" in st.session_state and (
        "selected_year" not in st.session_state or "selected_month" not in st.session_state
    ):
        st.warning("⚠️ Please select both a year and month for the Commission Date before proceeding.")
        should_stop_processing = True
    
    # Stop processing if needed
    if should_stop_processing or "confirmed_file_bytes" not in st.session_state:
        if "confirmed_file_bytes" not in st.session_state:
            st.warning("Please upload and confirm a file to proceed.")
        return

    st.markdown("---")
    st.subheader("Step 3: Loaded and Enriched Data")
    file_type = st.session_state["selected_file_type"]
    file_name = st.session_state["confirmed_file_name"]
    mime_type = st.session_state["confirmed_file_type"]
    file_bytes = st.session_state["confirmed_file_bytes"]
    
    # Get year and month for commission date
    year = st.session_state["selected_year"]
    month = st.session_state["selected_month"]
    month_num = st.session_state["selected_month_num"]

    st.write(f"### Processing: {file_name} (Type: {file_type})")

    try:
        debug_info = []
        # For Summit Medical PDFs
        if file_type == "Summit Medical" and mime_type == "application/pdf":
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name
            try:
                df = load_pdf_file_summit_medical(tmp_file_path)
            finally:
                os.remove(tmp_file_path)
        else:
            # For all Excel files
            suffix = ".xlsx" if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" else None
            extension = suffix if suffix else ".pdf"
            with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name
            try:
                # Pass year and month to all loaders
                df = load_file(
                    tmp_file_path, 
                    file_type, 
                    debug_info, 
                    year=str(year), 
                    month=month
                )
            finally:
                os.remove(tmp_file_path)

        # Add Commission Date columns for file types that don't already handle it
        if not LOADERS_WITH_DATE_HANDLING.get(file_type, False):
            df = add_commission_date_columns(df, year, month, month_num)

        # Validate file format
        is_valid, missing_columns = validate_file_format(df, file_type)
        if not is_valid:
            expected_list = "\n".join(f'"{col}"' for col in EXPECTED_COLUMNS.get(file_type, []))
            st.error(
                f"**The file uploaded does not match the expected format.**\n\n"
                f"Please check that you have selected the correct product line and associated file.\n\n"
                f"**Expected columns for {file_type}:**\n{expected_list}\n\n"
                f"**Missing columns:** {', '.join(missing_columns)}"
            )
            return

        # Check for valid Sales Rep names
        missing_names = check_for_valid_sales_rep(df)
        if missing_names:
            st.error("The following sales reps don't have any commission tier setup: " + ", ".join(missing_names))
            st.warning("Please set up commission tiers for these sales reps in the Portfolio Management section before proceeding.")
            
            # Add a link to the Portfolio Management page
            if st.button("Go to Portfolio Management"):
                st.rerun()
            
            # Stop further processing
            return

        # Check for Amount Line issues in QuickBooks
        amount_line_issues = []
        if file_type == "QuickBooks":
            amount_line_issues = check_for_amount_line_issues(df)
            if amount_line_issues:
                st.error("Some rows in the QuickBooks file have 'Amount line' ≤ 0. Please review them.")
                rows_str = ", ".join(map(str, amount_line_issues))
                st.markdown(f"**Row(s):** {rows_str}")

        # Configure Column Constraints for Editable DataFrame
        rep_column = None
        if "Sales Rep Name" in df.columns:
            rep_column = "Sales Rep Name"
        elif "Sales Rep" in df.columns:
            rep_column = "Sales Rep"

        col_config = {}
        if rep_column:
            rep_options = get_unique_sales_rep_names()
            col_config[rep_column] = st.column_config.SelectboxColumn(
                rep_column,
                options=rep_options,
                help="Select a Sales Rep from the list"
            )

        # Render the editable data editor with column configuration
        unique_key = f"editor_{file_name}_{file_type}"
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=False,
            key=unique_key,
            column_config=col_config
        )

        st.session_state.dataframes[file_name] = (edited_df, file_type)

    except Exception as e:
        st.error(f"Error loading {file_name} of type {file_type}: {e}")
        return

    # Create a visible separation for the action section
    st.markdown("---")
    st.subheader("Step 4: Save Data to Database")
    
    # Create two columns for layout
    col1, col2 = st.columns([3, 1])
    
    # Always show the warning in the first column if applicable
    with col1:
        if st.session_state.showing_overwrite_warning:
            st.warning("⚠️ Warning: Existing data will be overwritten!")
            st.markdown("The following data already exists in the database:")
            for msg in st.session_state.overwrite_messages:
                st.markdown(f"- {msg}")
    
    # Always show the buttons in the second column
    with col2:
        # Main save button with dynamic label based on warning state
        save_button_label = "Confirm and Save to Database"
        if st.session_state.showing_overwrite_warning:
            save_button_label = "Yes, Overwrite Data"
        
        if st.button(save_button_label, key="save_button", use_container_width=True):
            if not st.session_state.dataframes:
                st.warning("No data available to save. Please upload and process files first.")
                return

            # Check for blank cells in required fields
            invalid_files = {}
            for f_name, (df_data, f_type) in st.session_state.dataframes.items():
                blank_details = check_for_blanks_with_details(df_data, f_type)
                if blank_details:
                    invalid_files[f_name] = blank_details

            if invalid_files:
                st.error("Some files contain rows with blank values. Please fix them and try again.")
                for fname, row_col_details in invalid_files.items():
                    for row, cols in row_col_details:
                        st.markdown(f"- **File:** {fname} | **Row:** {row} | **Columns:** {', '.join(cols)}")
                return
            
            # If we're not already showing the warning, check for data to overwrite
            if not st.session_state.showing_overwrite_warning:
                overwrite_needed = False
                overwrite_messages = []
                
                for f_name, (df_data, f_type) in st.session_state.dataframes.items():
                    has_existing, date_info = check_for_existing_data(df_data, f_type)
                    if has_existing:
                        overwrite_needed = True
                        overwrite_messages.append(f"**{f_type}**: {date_info}")
                
                # If overwrite is needed, show the warning and return without saving
                if overwrite_needed:
                    st.session_state.showing_overwrite_warning = True
                    st.session_state.overwrite_messages = overwrite_messages
                    st.rerun()  # Force refresh to show the warning
                    return  # Return without saving
            
            # If we get here, either there's no data to overwrite or the user has confirmed
            # Proceed with saving all dataframes
            
            # Reset the warning state
            st.session_state.showing_overwrite_warning = False
            st.session_state.overwrite_messages = []
            
            # Save all the dataframes
            debug_output = []
            for f_name, (df_data, f_type) in st.session_state.dataframes.items():
                try:
                    # Dispatch to appropriate save function based on file type
                    save_functions = {
                        "Cygnus": save_cygnus_to_db,
                        "Logiquip": save_logiquip_to_db,
                        "Summit Medical": save_summit_medical_to_db,
                        "QuickBooks": save_quickbooks_to_db,
                        "InspeKtor": save_inspektor_to_db,
                        "Sunoptic": save_sunoptic_to_db,
                        "Ternio": save_ternio_to_db,
                        "Novo": save_novo_to_db,
                        "Chemence": save_chemence_to_db
                    }
                    
                    table_names = {
                        "Cygnus": "master_cygnus_sales",
                        "Logiquip": "master_logiquip_sales",
                        "Summit Medical": "master_summit_medical_sales",
                        "QuickBooks": "master_quickbooks_sales",
                        "InspeKtor": "master_inspektor_sales",
                        "Sunoptic": "master_sunoptic_sales",
                        "Ternio": "master_ternio_sales",
                        "Novo": "master_novo_sales",
                        "Chemence": "master_chemence_sales"
                    }
                    
                    if f_type in save_functions:
                        debug_output.extend(save_functions[f_type](df_data, table_names[f_type]))
                        st.success(f"Data from '{f_name}' successfully saved to the '{f_type}' table.")
                    else:
                        st.error(f"No save function defined for file type: {f_type}")
                    
                except Exception as e:
                    st.error(f"Error saving '{f_name}' to the database: {e}")

            # Display debug output
            if debug_output:
                st.markdown("### Debug Log")
                for message in debug_output:
                    st.markdown(f"- {message}")
            else:
                st.info("No debug messages to display.")
                
        # Only show Cancel button if warning is active
        if st.session_state.showing_overwrite_warning:
            if st.button("Cancel", key="cancel_button", use_container_width=True):
                st.session_state.showing_overwrite_warning = False
                st.session_state.overwrite_messages = []
                st.success("Operation cancelled. No data was modified.")
                st.rerun()  # Refresh to remove the warning

def data_upload_status_tab():
    st.title("Data Upload Status")
    data_status = fetch_table_data("data_status")
    if data_status.empty:
        st.warning("No data available in the data_status table. Initializing with default structure.")
        data_status = pd.DataFrame({
            "Product line": pd.Series(dtype='str'),
            "January": pd.Series(dtype='bool'),
            "February": pd.Series(dtype='bool'),
            "March": pd.Series(dtype='bool'),
            "April": pd.Series(dtype='bool'),
            "May": pd.Series(dtype='bool'),
            "June": pd.Series(dtype='bool'),
            "July": pd.Series(dtype='bool'),
            "August": pd.Series(dtype='bool'),
            "September": pd.Series(dtype='bool'),
            "October": pd.Series(dtype='bool'),
            "November": pd.Series(dtype='bool'),
            "December": pd.Series(dtype='bool')
        })

    st.subheader("Data Status (Editable)")
    boolean_columns = [col for col in data_status.columns if col != "Product line"]
    for col in boolean_columns:
        data_status[col] = data_status[col].fillna(False).astype(bool)
    data_status["Product line"] = data_status["Product line"].astype(str)

    edited_data = st.data_editor(
        data_status,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=False,
        key="data_status_editor"
    )

    if "save_initiated" not in st.session_state:
        st.session_state.save_initiated = False

    if st.button("Confirm and Upload to Database"):
        st.session_state.save_initiated = True
        st.warning("Are you sure you want to replace the current data with the new changes?")

    if st.session_state.save_initiated:
        if st.button("Yes, Replace Table"):
            for col in boolean_columns:
                edited_data[col] = edited_data[col].astype(bool)
            update_table_data("data_status", edited_data)
            st.session_state.save_initiated = False

# Create two tabs in the UI
tab1, tab2 = st.tabs(["Sales Data Upload", "Data Upload Status"])

with tab1:
    sales_data_tab()

with tab2:
    data_upload_status_tab()