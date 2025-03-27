import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def clean_string_value(value):
    """Clean string values by stripping whitespace and handling None/NaN."""
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()

def clean_dataframe(df):
    """Apply string cleaning to all string/object columns in a DataFrame."""
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = df[column].apply(clean_string_value)
    return df

# Load environment variables from .env file
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def fetch_table_data(table_name):
    """Fetch data from a PostgreSQL table with specified column order."""
    if table_name == "sales_rep_commission_tier":
        query = """
            SELECT "Sales Rep Name", "Rep Category", "Commission tier 1 rate", "Commission tier 2 rate"
            FROM sales_rep_commission_tier;
        """
    else:
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

def update_table_data(table_name, df):
    """Update the PostgreSQL table with the modified DataFrame."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            with conn.begin():
                # Clear the existing table
                conn.execute(text(f"DELETE FROM {table_name};"))
                # Insert the updated data
                df.to_sql(table_name, con=engine, if_exists="append", index=False)
        st.success(f"Changes successfully saved to the {table_name} table!")
    except Exception as e:
        st.error(f"Error updating the {table_name} table: {e}")
    finally:
        engine.dispose()

def render_preview_table(df, css_class=""):
    """Render a DataFrame as an HTML table without the index.
    Optionally, include a CSS class for custom styling.
    """
    html_table = df.reset_index(drop=True).to_html(index=False, classes=css_class)
    st.markdown(html_table, unsafe_allow_html=True)

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
    
def validate_sales_territory_upload(df, sales_rep_names):
    """
    Validate Sales Territory uploaded data for compliance:
    1. Check column names and number
    2. Check for blank cells (except "Valid until")
    3. Validate date formats for "Valid from" and "Valid until"
    4. Ensure Sales Rep names exist in the master list
    
    Returns:
        tuple: (is_valid, error_messages)
    """
    validation_errors = []
    is_valid = True
    
    # 1. Check required column names and number
    required_columns = ["Source", "Customer field", "Data field value", "Sales Rep name", "Valid from"]
    optional_columns = ["Valid until"]
    
    # Check all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        is_valid = False
    
    # Check if there are any unexpected columns
    expected_columns = required_columns + optional_columns
    unexpected_columns = [col for col in df.columns if col not in expected_columns]
    if unexpected_columns:
        validation_errors.append(f"Unexpected columns found: {', '.join(unexpected_columns)}")
        is_valid = False
    
    # If column validation failed, return early
    if not is_valid:
        return is_valid, validation_errors
    
    # 2. Check for blank cells in required columns
    for col in required_columns:
        blank_rows = df[df[col].isnull() | (df[col].astype(str).str.strip() == "")].index.tolist()
        if blank_rows:
            row_numbers = [str(i + 2) for i in blank_rows]  # +2 for excel row number (header + 1-based index)
            validation_errors.append(f"Empty values found in '{col}' column at rows: {', '.join(row_numbers)}")
            is_valid = False
    
    # 3. Validate date formats
    date_columns = ["Valid from", "Valid until"]
    date_regex = r"^\d{4}-\d{2}-\d{2}$"
    
    for col in date_columns:
        if col not in df.columns:
            continue
            
        # For Valid until, we need to filter out NaN values first
        if col == "Valid until":
            invalid_date_rows = df[~df[col].isnull() & ~df[col].astype(str).str.match(date_regex)].index.tolist()
        else:
            invalid_date_rows = df[~df[col].astype(str).str.match(date_regex)].index.tolist()
            
        if invalid_date_rows:
            row_numbers = [str(i + 2) for i in invalid_date_rows]  # +2 for excel row number
            validation_errors.append(f"Invalid date format in '{col}' column at rows: {', '.join(row_numbers)}. Format should be YYYY-MM-DD.")
            is_valid = False
    
    # 4. Validate Sales Rep names exist in the master list
    if "Sales Rep name" in df.columns:
        invalid_rep_names = []
        rep_name_errors = []
        
        for idx, row in df.iterrows():
            rep_name = str(row["Sales Rep name"]).strip()
            if rep_name not in sales_rep_names:
                invalid_rep_names.append(rep_name)
                rep_name_errors.append(f"Row {idx + 2}: '{rep_name}'")
        
        if invalid_rep_names:
            validation_errors.append(f"Sales Rep names not found in commission tier table: {', '.join(rep_name_errors)} --- Please add those names with commissions to your 'Sales Reps' table before proceeding.")
            is_valid = False
    
    return is_valid, validation_errors

# Initialize session state flags for editing if not already set.
if "service_editing" not in st.session_state:
    st.session_state.service_editing = False
if "commission_editing" not in st.session_state:
    st.session_state.commission_editing = False
if "territory_editing" not in st.session_state:
    st.session_state.territory_editing = False

# Also initialize session state variables to store the loaded file (if any)
if "loaded_service_df" not in st.session_state:
    st.session_state.loaded_service_df = None
if "loaded_commission_df" not in st.session_state:
    st.session_state.loaded_commission_df = None
if "loaded_sales_rep_df" not in st.session_state:
    st.session_state.loaded_sales_rep_df = None

# Streamlit UI: three tabs
tab1, tab2, tab3 = st.tabs(["Service to Product", "Sales Reps", "Sales Territory"])

#####################################
# Tab 1: Service-to-Product Mapping #
#####################################
with tab1:
    st.header("Service-to-Product Mapping - QuickBooks")
    
    if not st.session_state.service_editing:
        # Preview mode (read-only)
        try:
            df_service_to_product = fetch_table_data("service_to_product")
            st.subheader("Preview Service-to-Product Data (Read-Only)")

            # Place the Edit Data button right after the subheader
            if st.button("Edit Data", key="service_edit_button"):
                st.session_state.service_editing = True
            st.dataframe(df_service_to_product, use_container_width=True, hide_index=True, height=600)

        except Exception as e:
            st.error(f"Error: {e}")
        # if st.button("Edit Data", key="service_edit_button"):
        #     st.session_state.service_editing = True
    else:
        # Editing mode
        st.subheader("Edit Service-to-Product Data")
        
        # --- Load from File Section ---
        uploaded_file_service = st.file_uploader(
            "Upload a .xlsx file to load data for Service-to-Product Mapping", 
            type=["xlsx"],
            key="service_file_uploader"
        )
        if uploaded_file_service is not None:
            if st.button("Load from File", key="service_load_file_button"):
                try:
                    df_from_file = pd.read_excel(uploaded_file_service)
                    # Instead of updating the DB immediately, store it in session state.
                    st.session_state.loaded_service_df = df_from_file
                    st.success("File loaded successfully. You can now edit the data below.")
                except Exception as e:
                    st.error(f"Error loading file: {e}")
        
        # Determine which dataframe to show in the editor:
        base_df = st.session_state.loaded_service_df if st.session_state.loaded_service_df is not None else fetch_table_data("service_to_product")
        
        # Show the editable table (index hidden via hide_index=True)
        try:
            edited_df = st.data_editor(
                base_df,
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                key="service_to_product_editor",
                height=600
            )
            if "service_save_initiated" not in st.session_state:
                st.session_state.service_save_initiated = False

            if st.button("Save Changes", key="service_save_button"):
                st.session_state.service_save_initiated = True
                st.warning("Are you sure you want to replace the current table with the new data?")
            # if st.session_state.service_save_initiated:
            #     if st.button("Yes, Replace Table", key="service_confirm_button"):
            #         update_table_data("service_to_product", edited_df)
            #         st.session_state.service_save_initiated = False
            #         # Clear the loaded file if it was used
            #         st.session_state.loaded_service_df = None
            ### SAVING WITH STRIPPING GUARDRAILS
            if st.session_state.service_save_initiated:
                if st.button("Yes, Replace Table", key="service_confirm_button"):
                    # Clean the data before saving
                    edited_df = clean_dataframe(edited_df)
                    update_table_data("service_to_product", edited_df)
                    st.session_state.service_save_initiated = False
                    # Clear the loaded file if it was used
                    st.session_state.loaded_service_df = None
        except Exception as e:
            st.error(f"Error: {e}")
            
        # Button to cancel editing and return to preview mode
        if st.button("Cancel Editing", key="service_cancel_button"):
            st.session_state.service_editing = False
            st.session_state.loaded_service_df = None  # Clear any loaded file

##########################################
# Tab 2: Sales Reps Commission Tiers     #
##########################################
with tab2:
    st.header("Sales Reps Commission Tiers")
    
    if not st.session_state.commission_editing:
        # Preview mode (read-only)
        try:
            df_commission = fetch_table_data("sales_rep_commission_tier")
            # Ensure the "Rep Category" column exists for preview.
            if "Rep Category" not in df_commission.columns:
                df_commission["Rep Category"] = ""
            st.subheader("Preview Sales Reps Commission Data (Read-Only)")

            if st.button("Edit Data", key="commission_edit_button"):
                st.session_state.commission_editing = True
            st.dataframe(df_commission, use_container_width=True, hide_index=True, height=600)
        except Exception as e:
            st.error(f"Error: {e}")

    else:
        # Editing mode
        st.subheader("Edit Sales Reps Commission Data")
        
        # --- Load from File Section ---
        uploaded_file_commission = st.file_uploader(
            "Upload a .xlsx file to load data for Sales Reps Commission Tiers", 
            type=["xlsx"],
            key="commission_file_uploader"
        )
        if uploaded_file_commission is not None:
            if st.button("Load from File", key="commission_load_file_button"):
                try:
                    df_from_file = pd.read_excel(uploaded_file_commission)
                    st.session_state.loaded_commission_df = df_from_file
                    st.success("File loaded successfully. You can now edit the data below.")
                except Exception as e:
                    st.error(f"Error loading file: {e}")
                    
        # Determine which DataFrame to show in the editor:
        base_df = (
            st.session_state.loaded_commission_df 
            if st.session_state.loaded_commission_df is not None 
            else fetch_table_data("sales_rep_commission_tier")
        )
        
        try:
            # Ensure the "Rep Category" column exists.
            if "Rep Category" not in base_df.columns:
                base_df["Rep Category"] = ""
            # Convert commission rate columns to float.
            base_df["Commission tier 1 rate"] = base_df["Commission tier 1 rate"].astype(float)
            base_df["Commission tier 2 rate"] = base_df["Commission tier 2 rate"].astype(float)
            
            # Reorder columns into the desired order.
            ordered_columns = [
                "Sales Rep Name", 
                "Rep Category", 
                "Commission tier 1 rate", 
                "Commission tier 2 rate"
            ]
            base_df = base_df[ordered_columns]
            
            # Show the editable table.
            edited_df = st.data_editor(
                base_df,
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                key="commission_editor"
            )
            
            if "save_initiated" not in st.session_state:
                st.session_state.save_initiated = False

            if st.button("Save Commission Changes"):
                # Validate that every row has a non-empty Sales Rep Name.
                if edited_df["Sales Rep Name"].isnull().any() or (edited_df["Sales Rep Name"].astype(str).str.strip() == "").any():
                    st.error("Every row must have a Sales Rep Name. Please complete missing entries.")
                # Validate that every row has a Commission tier 1 rate.
                elif edited_df["Commission tier 1 rate"].isnull().any():
                    st.error("Every row must have a Commission tier 1 rate. Please complete missing entries.")
                # Validate that commission rates are valid percentages.
                elif not edited_df["Commission tier 1 rate"].between(0, 100).all() or not edited_df["Commission tier 2 rate"].between(0, 100).all():
                    st.error("Commission rates must be valid percentages (0 to 100).")
                else:
                    st.session_state.save_initiated = True
                    st.warning("Are you sure you want to replace the current table with the new data?")
                    
            # if st.session_state.save_initiated:
            #     if st.button("Yes, Replace Table", key="commission_confirm_button"):
            #         # Reorder the edited DataFrame as well, just to be sure.
            #         edited_df = edited_df[ordered_columns]
            #         update_table_data("sales_rep_commission_tier", edited_df)
            #         st.session_state.save_initiated = False
            #         st.session_state.loaded_commission_df = None
            ### SAVING WITH STRIPPING GUARDRAILS
            if st.session_state.save_initiated:
                if st.button("Yes, Replace Table", key="commission_confirm_button"):
                    # Reorder the edited DataFrame as well, just to be sure.
                    edited_df = edited_df[ordered_columns]
                    # Clean the data before saving
                    edited_df = clean_dataframe(edited_df)
                    update_table_data("sales_rep_commission_tier", edited_df)
                    st.session_state.save_initiated = False
                    st.session_state.loaded_commission_df = None
        except Exception as e:
            st.error(f"Error: {e}")
        
        if st.button("Cancel Editing", key="commission_cancel_button"):
            st.session_state.commission_editing = False
            st.session_state.loaded_commission_df = None

#####################################
# Tab 3: Sales Territory            #
#####################################
with tab3:
    st.header("Sales Territory")
    
    if not st.session_state.territory_editing:
        # Preview mode (read-only)
        try:
            df_sales_rep = fetch_table_data("master_sales_rep")
            # Convert the date columns if they exist
            if "Valid from" in df_sales_rep.columns:
                df_sales_rep["Valid from"] = pd.to_datetime(df_sales_rep["Valid from"]).dt.strftime('%Y-%m-%d')
            if "Valid until" in df_sales_rep.columns:
                df_sales_rep["Valid until"] = pd.to_datetime(df_sales_rep["Valid until"]).dt.strftime('%Y-%m-%d')
            
            st.subheader("Preview Master Sales Rep Data (Read-Only)")
            if st.button("Edit Data", key="territory_edit_button"):
                st.session_state.territory_editing = True
            st.dataframe(df_sales_rep, use_container_width=True, hide_index=True, height=600)
        except Exception as e:
            st.error(f"Error: {e}")

    else:
        # Editing mode
        st.subheader("Edit Master Sales Rep Data")
        
        # --- Load from File Section ---
        uploaded_file_sales_rep = st.file_uploader(
            "Upload a .xlsx file to load data for Sales Territory", 
            type=["xlsx"],
            key="sales_rep_file_uploader"
        )
        
        # Initialize validation error state in session state if not exist
        if "territory_validation_errors" not in st.session_state:
            st.session_state.territory_validation_errors = []
        
        if uploaded_file_sales_rep is not None:
            if st.button("Load from File", key="sales_rep_load_file_button"):
                try:
                    # Load the file
                    df_from_file = pd.read_excel(uploaded_file_sales_rep)
                    
                    # Get valid Sales Rep names for validation
                    sales_rep_names = get_unique_sales_rep_names()
                    
                    # Validate the dataframe
                    is_valid, validation_errors = validate_sales_territory_upload(df_from_file, sales_rep_names)
                    
                    if is_valid:
                        st.session_state.loaded_sales_rep_df = df_from_file
                        st.success("File loaded and validated successfully. You can now edit the data below.")
                        st.session_state.territory_validation_errors = []
                    else:
                        st.session_state.territory_validation_errors = validation_errors
                        # Don't display errors here - we'll display them once in the dedicated error section below
                except Exception as e:
                    st.error(f"Error loading file: {e}")
        
        # Display validation errors if any
        if st.session_state.territory_validation_errors:
            st.error("Please fix the following validation errors before proceeding:")
            for error in st.session_state.territory_validation_errors:
                st.warning(error)
        
        # Determine which dataframe to show in the editor:
        base_df = st.session_state.loaded_sales_rep_df if st.session_state.loaded_sales_rep_df is not None else fetch_table_data("master_sales_rep")
        if "Valid from" in base_df.columns:
            base_df["Valid from"] = pd.to_datetime(base_df["Valid from"]).dt.strftime('%Y-%m-%d')
        if "Valid until" in base_df.columns:
            base_df["Valid until"] = pd.to_datetime(base_df["Valid until"]).dt.strftime('%Y-%m-%d')
        
        # Get Sales Rep Names for dropdown
        sales_rep_names = get_unique_sales_rep_names()
        
        # Create column config with Sales Rep name dropdown
        column_config = {
            "Sales Rep name": st.column_config.SelectboxColumn(
                "Sales Rep name",
                options=sales_rep_names,
                help="Select a Sales Rep name from the list"
            ),
            "Valid from": st.column_config.TextColumn(
                "Valid from",
                help="Date format YYYY-MM-DD"
            ),
            "Valid until": st.column_config.TextColumn(
                "Valid until",
                help="Date format YYYY-MM-DD (can be empty)"
            )
        }
        
        try:
            edited_df = st.data_editor(
                base_df,
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                key="sales_rep_editor",
                column_config=column_config
            )
            
            if "territory_save_initiated" not in st.session_state:
                st.session_state.territory_save_initiated = False

            if st.button("Save Changes", key="territory_save_button"):
                # Validate the edited data before saving
                is_valid, validation_errors = validate_sales_territory_upload(edited_df, sales_rep_names)
                
                if is_valid:
                    st.session_state.territory_save_initiated = True
                    st.warning("Are you sure you want to replace the current table with the new data?")
                    st.session_state.territory_validation_errors = []
                else:
                    st.session_state.territory_validation_errors = validation_errors
                    st.error("Validation failed. Please fix the errors before saving:")
                    for error in validation_errors:
                        st.warning(error)
            
            if st.session_state.territory_save_initiated:
                if st.button("Yes, Replace Table", key="territory_confirm_button"):
                    # Clean the data before saving
                    edited_df = clean_dataframe(edited_df)
                    
                    # Convert date columns to datetime format for database
                    if "Valid from" in edited_df.columns:
                        edited_df["Valid from"] = pd.to_datetime(edited_df["Valid from"])
                    if "Valid until" in edited_df.columns:
                        edited_df["Valid until"] = pd.to_datetime(edited_df["Valid until"], errors='coerce')
                    
                    update_table_data("master_sales_rep", edited_df)
                    st.session_state.territory_save_initiated = False
                    st.session_state.loaded_sales_rep_df = None
                    st.session_state.territory_validation_errors = []
                    
                    # Reload the page to show updated data
                    st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")
        
        if st.button("Cancel Editing", key="territory_cancel_button"):
            st.session_state.territory_editing = False
            st.session_state.loaded_sales_rep_df = None
            st.session_state.territory_validation_errors = []