import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import re

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_unique_sales_reps():
    """Fetch distinct Sales Rep Names from the harmonised_table."""
    query = """
        SELECT DISTINCT TRIM("Sales Rep") AS "Sales Rep"
        FROM harmonised_table
        WHERE "Sales Rep" IS NOT NULL
        ORDER BY TRIM("Sales Rep")
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            sales_reps = {row[0].strip() for row in result.fetchall() if row[0] is not None}
        return sorted(sales_reps)
    except Exception as e:
        st.error(f"Error fetching Sales Reps: {e}")
        return []
    finally:
        engine.dispose()

def get_product_lines_with_friendly_names():
    """Fetch product lines and map them to user-friendly names."""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'master\_%\_sales'
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            table_names = [row[0] for row in result.fetchall()]
        def format_product_line_name(table_name):
            product_line = re.sub(r"^master_|_sales$", "", table_name)
            return f"{product_line.replace('_', ' ').title()} Commission Report"
        return {table: format_product_line_name(table) for table in table_names}
    except Exception as e:
        st.error(f"Error fetching product lines from tables: {e}")
        return {}
    finally:
        engine.dispose()

# def fetch_table_data(product_line, selected_sales_reps):
#     """Fetch sales data for the selected product line and sales reps."""
#     table_name = product_line  # The selected table name
#     # Use the correct column name based on your schema:
#     sales_rep_column = "Sales Rep Name"  

#     # Construct the filter query for sales reps.
#     sales_rep_filter = " OR ".join([f'"{sales_rep_column}" = :rep{i}' for i in range(len(selected_sales_reps))])
#     ordering_column = get_valid_ordering_column(table_name) or sales_rep_column

#     query = f"""
#         SELECT *
#         FROM {table_name}
#         WHERE ({sales_rep_filter})
#         ORDER BY "{ordering_column}", "{sales_rep_column}"
#     """
#     params = {f"rep{i}": rep for i, rep in enumerate(selected_sales_reps)}

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), params)
#             df = pd.DataFrame(result.fetchall(), columns=result.keys())
#         # Exclude specific columns if they exist.
#         columns_to_exclude = ["row_hash", "SteppingStone", "Margin"]
#         df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns], errors="ignore")
#         return df
#     except Exception as e:
#         st.error(f"Error fetching data from table '{table_name}': {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()
def fetch_table_data(product_line, selected_sales_reps):
    """Fetch sales data for the selected product line and sales reps."""
    table_name = product_line  # The selected table name
    # Use the correct column name based on your schema:
    sales_rep_column = "Sales Rep Name"  

    # Construct the filter query for sales reps.
    sales_rep_filter = " OR ".join([f'"{sales_rep_column}" = :rep{i}' for i in range(len(selected_sales_reps))])
    ordering_column = get_valid_ordering_column(table_name) or sales_rep_column

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE ({sales_rep_filter})
        ORDER BY "{ordering_column}", "{sales_rep_column}"
    """
    params = {f"rep{i}": rep for i, rep in enumerate(selected_sales_reps)}

    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Exclude specific columns if they exist.
        columns_to_exclude = ["row_hash", "SteppingStone", "Margin"]
        df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns], errors="ignore")
        
        # Special handling for Sunoptic table - reorder columns
        if table_name == "master_sunoptic_sales" and "Invoice Date" in df.columns:
            # Check if the Commission Date columns exist
            if "Commission Date YYYY" in df.columns and "Commission Date MM" in df.columns:
                # Get all columns in their current order
                all_columns = df.columns.tolist()
                
                # Remove the Commission Date columns from their current positions
                all_columns.remove("Commission Date YYYY")
                all_columns.remove("Commission Date MM")
                
                # Find the position of Invoice Date
                invoice_date_pos = all_columns.index("Invoice Date")
                
                # Insert the Commission Date columns after Invoice Date
                all_columns.insert(invoice_date_pos + 1, "Commission Date YYYY")
                all_columns.insert(invoice_date_pos + 2, "Commission Date MM")
                
                # Reorder the DataFrame columns
                df = df[all_columns]
        
        return df
    except Exception as e:
        st.error(f"Error fetching data from table '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def get_valid_ordering_column(table_name):
    """Fetch the first valid date column from the table schema."""
    possible_columns = ["ClosedDate", "Date Paid", "Invoice Date", "Date"]
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 0"))
            columns = result.keys()
            for col in possible_columns:
                if col in columns:
                    return col
        return None
    except Exception as e:
        st.error(f"Error detecting valid ordering column for table '{table_name}': {e}")
        return None
    finally:
        engine.dispose()

# Fetch product lines with user-friendly names.
product_line_mapping = get_product_lines_with_friendly_names()
product_lines_display = list(product_line_mapping.values())

# Streamlit UI
st.title("Sales History")

col1, col2 = st.columns(2)

# --- Column 1: Sales Rep Selection ---
with col1:
    sales_reps = get_unique_sales_reps()
    # Apply restriction if the logged-in user is a simple user.
    if "user_permission" in st.session_state and st.session_state.user_permission.lower() == "user":
        user_name = st.session_state.user_name
        if user_name in sales_reps:
            sales_reps = ["All", user_name]
        else:
            sales_reps = ["All"]
    else:
        sales_reps.insert(0, "All")  # For Admins, include full list.
    
    selected_sales_rep = st.selectbox("Select Sales Rep:", sales_reps)

# --- Column 2: Product Line Selection (with friendly names) ---
with col2:
    selected_product_line_display = st.selectbox("Select Data Source:", product_lines_display)

# Convert user-friendly name back to table name.
selected_product_line = next((k for k, v in product_line_mapping.items() if v == selected_product_line_display), None)

# --- Data Filtering ---
if selected_sales_rep and selected_product_line:
    if selected_sales_rep == "All":
        selected_sales_reps = sales_reps[1:]  # Exclude "All"
    else:
        selected_sales_reps = [selected_sales_rep]

    # Fetch filtered data.
    sales_history_df = fetch_table_data(selected_product_line, selected_sales_reps)
    # Format date columns if present.
    if "Date YYYY" in sales_history_df.columns:
        sales_history_df["Date YYYY"] = sales_history_df["Date YYYY"].apply(lambda x: str(int(x)))
    if "Date MM" in sales_history_df.columns:
        sales_history_df["Date MM"] = sales_history_df["Date MM"].apply(lambda x: f"{int(x):02d}")

    # Display the DataFrame.
    if not sales_history_df.empty:
        # Convert "Num" column to plain text if the selected data source is QuickBooks Commission Report.
        if selected_product_line_display.lower().startswith("quickbooks"):
            if "Num" in sales_history_df.columns:
                sales_history_df["Num"] = sales_history_df["Num"].astype(str)
        st.subheader(f"Sales Data Source: {selected_product_line_display}")
        st.dataframe(sales_history_df, use_container_width=True, height=600, hide_index=True)
    else:
        st.warning(f"No data available for the selected criteria in '{selected_product_line_display}'.")
