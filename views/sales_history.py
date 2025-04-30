import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def check_table_exists(table_name):
    """Check if a table exists and has data."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # Check if table exists
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                return False, "Table does not exist"
            
            # Check if table has data
            count_query = text(f"SELECT COUNT(*) FROM {table_name}")
            result = conn.execute(count_query)
            count = result.scalar()
            return count > 0, f"Table exists with {count} records"
    except Exception as e:
        return False, str(e)
    finally:
        engine.dispose()

def get_unique_product_lines():
    """Fetch unique product lines from all master tables."""
    tables = [
        "master_logiquip_sales", 
        "master_cygnus_sales", 
        "master_summit_medical_sales", 
        "master_quickbooks_sales",
        "master_inspektor_sales",
        "master_sunoptic_sales",
        "master_ternio_sales",
        "master_novo_sales",
        "master_chemence_sales"
    ]
    
    product_lines = []
    engine = get_db_connection()
    
    try:
        with engine.connect() as conn:
            # Check tables exist and have data
            has_any_data = False
            for table in tables:
                exists, _ = check_table_exists(table)
                if exists:
                    has_any_data = True
                    product_lines.append(table.replace("master_", "").replace("_sales", "").capitalize())
            
            if not has_any_data:
                return []
                
        return sorted(product_lines)
    except Exception as e:
        st.error(f"Error fetching product lines: {e}")
        return []
    finally:
        engine.dispose()

def fetch_data_from_table(table_name, filters=None):
    """
    Fetch data from a specific table with optional filters.
    
    Args:
        table_name: The name of the table to query.
        filters: Dictionary with column names as keys and lists of values to filter by.
        
    Returns:
        pandas DataFrame with the query results.
    """
    engine = get_db_connection()
    
    # Check if table exists and has data
    table_has_data, message = check_table_exists(table_name)
    if not table_has_data:
        st.warning(f"No data available in {table_name}. {message}")
        return pd.DataFrame()
    
    try:
        with engine.connect() as conn:
            # Start building the query
            query = f"SELECT * FROM {table_name}"
            
            # Add WHERE clause if filters are provided and not empty
            where_conditions = []
            params = {}
            
            if filters:
                for column, values in filters.items():
                    if values and isinstance(values, list) and len(values) > 0:
                        if len(values) == 1:
                            where_conditions.append(f'"{column}" = :{column}')
                            params[column] = values[0]
                        else:
                            placeholders = [f':{column}_{i}' for i in range(len(values))]
                            where_conditions.append(f'"{column}" IN ({", ".join(placeholders)})')
                            for i, value in enumerate(values):
                                params[f'{column}_{i}'] = value
            
            # Only add WHERE clause if there are actual conditions
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Add ORDER BY clause - ensure we're not duplicating columns
            query += ' ORDER BY "Sales Rep Name"'
            
            # Execute the query
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
    except Exception as e:
        st.error(f"Error fetching data from table '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def get_column_values(table_name, column_name):
    """Get unique values from a column in a table."""
    engine = get_db_connection()
    
    # Check if table exists and has data
    table_has_data, _ = check_table_exists(table_name)
    if not table_has_data:
        return []
    
    try:
        with engine.connect() as conn:
            query = text(f'SELECT DISTINCT "{column_name}" FROM {table_name} WHERE "{column_name}" IS NOT NULL ORDER BY "{column_name}"')
            result = conn.execute(query)
            values = [row[0] for row in result.fetchall()]
            return values
    except Exception as e:
        st.error(f"Error fetching values for {column_name} from {table_name}: {e}")
        return []
    finally:
        engine.dispose()

def sales_history_page():
    st.title("Sales History")
    
    # Get unique product lines
    product_lines = get_unique_product_lines()
    
    if not product_lines:
        st.info("No sales data available yet. Please upload data through the Sales Data Upload section first.")
        return
    
    # Select product line
    selected_product_line = st.selectbox("Select Product Line:", product_lines)
    
    if not selected_product_line:
        st.warning("Please select a product line to view sales history.")
        return
    
    # Map product line to table name
    table_name = f"master_{selected_product_line.lower()}_sales"
    
    # Initialize filters
    filters = {}
    
    # Apply different filter options based on product line
    with st.expander("Filters", expanded=True):
        # Common filter for Sales Rep Name
        sales_rep_names = get_column_values(table_name, "Sales Rep Name")
        if sales_rep_names:
            selected_sales_reps = st.multiselect("Sales Rep Name:", sales_rep_names)
            if selected_sales_reps:
                filters["Sales Rep Name"] = selected_sales_reps
        
        # Different filters for different product lines
        if selected_product_line == "Cygnus":
            states = get_column_values(table_name, "State")
            if states:
                selected_states = st.multiselect("State:", states)
                if selected_states:
                    filters["State"] = selected_states
                    
            # Add more Cygnus-specific filters as needed
            
        elif selected_product_line == "Logiquip":
            contracts = get_column_values(table_name, "Contract")
            if contracts:
                selected_contracts = st.multiselect("Contract:", contracts)
                if selected_contracts:
                    filters["Contract"] = selected_contracts
            
            # Add more Logiquip-specific filters as needed
            
        # Similar filters for other product lines
        # Add more conditions for other product lines
    
    # Fetch data with filters
    data = fetch_data_from_table(table_name, filters)
    
    if not data.empty:
        # Display data summary
        st.subheader(f"Data Summary ({len(data)} records)")
        
        # Format currency columns if they exist
        currency_columns = ["Invoice Total", "Sales Total", "Total Rep Due", "Comm Amt", "Commission"]
        for col in currency_columns:
            if col in data.columns:
                try:
                    data[col] = data[col].apply(
                        lambda x: f"${float(x):,.2f}" if pd.notnull(x) and x != "" else ""
                    )
                except:
                    pass
        
        # Display as an interactive table
        st.dataframe(data, use_container_width=True)
        
        # CSV Download option
        csv = data.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name=f"{selected_product_line}_sales_history.csv",
            mime="text/csv",
        )
    else:
        st.info(f"No data found for {selected_product_line} with the current filters.")

# Run the page
sales_history_page()