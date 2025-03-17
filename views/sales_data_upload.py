import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import tempfile

# Import your existing loaders
from data_loaders.cygnus.cygnus_loader import load_excel_file_cygnus
from data_loaders.logiquip.logiquip_loader import load_excel_file_logiquip
from data_loaders.summit_medical.summit_medical_loader import load_pdf_file_summit_medical
from data_loaders.quickbooks.quickbooks_loader import load_excel_file_quickbooks
from data_loaders.inspektor.inspektor_loader import load_excel_file_inspektor  # New import

# Import your existing db_utils
from data_loaders.cygnus.cygnus_db_utils import save_dataframe_to_db as save_cygnus_to_db
from data_loaders.logiquip.logiquip_db_utils import save_dataframe_to_db as save_logiquip_to_db
from data_loaders.summit_medical.summit_medical_db_utils import save_dataframe_to_db as save_summit_medical_to_db
from data_loaders.quickbooks.quickbooks_db_utils import save_dataframe_to_db as save_quickbooks_to_db
from data_loaders.inspektor.inspektor_db_utils import save_dataframe_to_db as save_inspektor_to_db

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

# Updated FILE_TYPES including Inspektor
FILE_TYPES = {
    "Logiquip": "Logiquip",
    "Cygnus": "Cygnus",
    "Summit Medical": "Summit Medical",
    "QuickBooks": "QuickBooks",
    "InspeKtor": "InspeKtor",  # New file type
}

def load_excel_file(filepath: str, file_type: str, debug_info: list) -> pd.DataFrame:
    """
    Generic dispatcher to the correct loader function, or a direct read_excel if none matched.
    `filepath` is the path to a temporary file on disk.
    """
    if file_type == "Cygnus":
        return load_excel_file_cygnus(filepath)
    elif file_type == "Logiquip":
        return load_excel_file_logiquip(filepath)
    elif file_type == "Summit Medical":
        # Should not reach here if we handle PDF logic separately, but let's keep it for safety.
        return load_pdf_file_summit_medical(filepath)
    elif file_type == "QuickBooks":
        return load_excel_file_quickbooks(filepath)
    elif file_type == "InspeKtor":  # New branch for Inspektor
        return load_excel_file_inspektor(filepath)
    else:
        return pd.read_excel(filepath)

def check_for_blanks_with_details(df: pd.DataFrame) -> list:
    """
    Return a list of (row_number, [columns_with_blanks]) for any row that has blank cells.
    """
    blank_details = []
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
            if row['Amount line'] <= 0:
                issues.append(row_idx + 1)  # Add 1 to row index to make it human-readable
    return issues

def fetch_table_data(table_name: str) -> pd.DataFrame:
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

# NEW helper: fetch unique Sales Rep names from sales_rep_commission_tier
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

# NEW: Helper function to validate Sales Rep names.
def check_for_valid_sales_rep(df: pd.DataFrame) -> list:
    """
    Check if the names in the raw DataFrame column "Sales Rep Name" are present
    in the sales_rep_commission_tier table (using the "Sales Rep Name" column).
    Returns a list of names that are missing.
    """
    if "Sales Rep Name" not in df.columns:
        return []  # or return an error message if this column is required
    valid_names = set(get_unique_sales_rep_names())
    df_names = set(df["Sales Rep Name"].dropna().unique())
    missing_names = list(df_names - valid_names)
    return missing_names

def sales_data_tab():
    st.title("Sales Data Upload Hub")

    # Initialize a place to store processed dataframes if not present
    if "dataframes" not in st.session_state:
        st.session_state.dataframes = {}

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Step 1: Select Product Line")
        # Instead of clearing the whole session state, only clear file-related keys.
        if st.button("Upload a New File"):
            keys_to_clear = [
                "selected_file_type",
                "confirmed_file_bytes",
                "confirmed_file_name",
                "confirmed_file_type",
                "dataframes"
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        selected_file_type = st.selectbox(
            "Choose the product line:",
            list(FILE_TYPES.keys()),
            help="Select the product line you want to process."
        )
        # If product line changes, clear previous file data.
        if st.session_state.get("selected_file_type") != selected_file_type:
            st.session_state["selected_file_type"] = selected_file_type
            st.session_state.pop("confirmed_file_bytes", None)
            st.session_state.pop("confirmed_file_name", None)
            st.session_state.pop("confirmed_file_type", None)

    with col2:
        st.subheader("Step 2: Upload a File to Process")
        uploaded_file = st.file_uploader("Upload a .xlsx or .pdf file:", type=["xlsx", "pdf"])
        if uploaded_file and st.button("Confirm File Selection"):
            file_bytes = uploaded_file.read()
            st.session_state["confirmed_file_bytes"] = file_bytes
            st.session_state["confirmed_file_name"] = uploaded_file.name
            st.session_state["confirmed_file_type"] = uploaded_file.type
            st.success(f"File '{uploaded_file.name}' has been confirmed!")

    if "confirmed_file_bytes" not in st.session_state:
        st.warning("Please upload and confirm a file to proceed.")
        return

    st.markdown("---")
    st.subheader("Step 3: Loaded and Enriched Data")
    file_type = st.session_state["selected_file_type"]
    file_name = st.session_state["confirmed_file_name"]
    mime_type = st.session_state["confirmed_file_type"]
    file_bytes = st.session_state["confirmed_file_bytes"]

    st.write(f"### Processing: {file_name} (Type: {file_type})")

    try:
        debug_info = []
        if file_type == "Summit Medical" and mime_type == "application/pdf":
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name
            try:
                df = load_pdf_file_summit_medical(tmp_file_path)
            finally:
                os.remove(tmp_file_path)
        else:
            suffix = ".xlsx" if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" else None
            extension = suffix if suffix else ".pdf"
            with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name
            try:
                df = load_excel_file(tmp_file_path, file_type, debug_info)
            finally:
                os.remove(tmp_file_path)

        numeric_columns = ["Net Sales Amount", "Comm Rate", "Comm $"]
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = df[col].astype("float64")
                except Exception as e:
                    st.error(f"Error casting column {col} to float: {e}")

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

        # NEW: Check for valid Sales Rep names
        missing_names = check_for_valid_sales_rep(df)
        if missing_names:
            st.error("The following sales reps don't have any commission tier setup: " + ", ".join(missing_names))
            # You can optionally return here to force the user to fix the issue before proceeding:
            #return

        amount_line_issues = []
        if file_type == "QuickBooks":
            amount_line_issues = check_for_amount_line_issues(df)
            if amount_line_issues:
                st.error("Some rows in the QuickBooks file have 'Amount line' ≤ 0. Please review them.")
                rows_str = ", ".join(map(str, amount_line_issues))
                st.markdown(f"**Row(s):** {rows_str}")

        # --- New: Configure Column Constraints for Editable DataFrame ---
        # Check if the DataFrame has a sales rep column.
        rep_column = None
        if "Sales Rep Name" in df.columns:
            rep_column = "Sales Rep Name"
        elif "Sales Rep" in df.columns:
            rep_column = "Sales Rep"

        col_config = {}
        if rep_column:
            rep_options = get_unique_sales_rep_names()  # This function defined above.
            col_config[rep_column] = st.column_config.SelectboxColumn(
                rep_column,
                options=rep_options,
                help="Select a Sales Rep from the list"
            )

        # Now render the editable data editor with our column configuration.
        unique_key = f"editor_{file_name}_{file_type}"
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=False,
            key=unique_key,
            column_config=col_config  # Apply our drop-down configuration
        )

        st.session_state.dataframes[file_name] = (edited_df, file_type)

    except Exception as e:
        st.error(f"Error loading {file_name} of type {file_type}: {e}")
        return

    if st.button("Confirm and Save to Database"):
        if not st.session_state.dataframes:
            st.warning("No data available to save. Please upload and process files first.")
            return

        invalid_files = {}
        for f_name, (df_data, _) in st.session_state.dataframes.items():
            blank_details = check_for_blanks_with_details(df_data)
            if blank_details:
                invalid_files[f_name] = blank_details

        if invalid_files:
            st.error("Some files contain rows with blank values. Please fix them and try again.")
            for fname, row_col_details in invalid_files.items():
                for row, cols in row_col_details:
                    st.markdown(f"- **File:** {fname} | **Row:** {row} | **Columns:** {', '.join(cols)}")
            return
            
        debug_output = []
        for f_name, (df_data, f_type) in st.session_state.dataframes.items():
            try:
                if f_type == "Cygnus":
                    debug_output.extend(save_cygnus_to_db(df_data, "master_cygnus_sales"))
                elif f_type == "Logiquip":
                    debug_output.extend(save_logiquip_to_db(df_data, "master_logiquip_sales"))
                elif f_type == "Summit Medical":
                    debug_output.extend(save_summit_medical_to_db(df_data, "master_summit_medical_sales"))
                elif f_type == "QuickBooks":
                    debug_output.extend(save_quickbooks_to_db(df_data, "master_quickbooks_sales"))
                elif f_type == "InspeKtor":
                    debug_output.extend(save_inspektor_to_db(df_data, "master_inspektor_sales"))
                # Optionally, you can add a similar saving function for Inspektor if needed.
                st.success(f"Data from '{f_name}' successfully saved to the '{f_type}' table.")
            except Exception as e:
                st.error(f"Error saving '{f_name}' to the database: {e}")

        if debug_output:
            st.markdown("### Debug Log")
            for message in debug_output:
                st.markdown(f"- {message}")
        else:
            st.info("No debug messages to display.")

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
