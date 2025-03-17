import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
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

def get_available_years():
    """Fetch distinct years from the harmonised_table."""
    query = """
        SELECT DISTINCT "Date YYYY" AS year
        FROM harmonised_table
        ORDER BY year
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            years = [row[0] for row in result.fetchall()]
        return years
    except Exception as e:
        st.error(f"Error fetching years: {e}")
        return []
    finally:
        engine.dispose()

def fetch_business_objective_data(selected_year):
    """Fetch and construct the business objective DataFrame with sub-totals."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # Fetch unique product lines and sales reps per product line
            sales_rep_query = """
                SELECT DISTINCT "Product line", "Sales Rep name"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
                ORDER BY "Product line", "Sales Rep name"
            """
            sales_reps = pd.read_sql_query(sales_rep_query, conn, params=(selected_year,))

            # Fetch commission tier thresholds
            commission_query = """
                SELECT "Product line", "Sales Rep name", "Commission tier threshold"
                FROM sales_rep_commission_tier_threshold
                WHERE "Year" = %s
            """
            commission_tiers = pd.read_sql_query(commission_query, conn, params=(selected_year,))

            # Fetch monthly objectives
            objective_query = """
                SELECT "Product line", "Sales Rep name", "Month", "Objective"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
            """
            objectives = pd.read_sql_query(objective_query, conn, params=(selected_year,))

        # If there are no sales reps, return an empty DataFrame with all expected columns.
        if sales_reps.empty:
            columns = [
                "Product line", "Sales Rep name", "Commission tier threshold",
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December",
                "Annual Objective"
            ]
            return pd.DataFrame(columns=columns)

        # Merge data to create the full DataFrame
        full_df = sales_reps.merge(commission_tiers, on=["Product line", "Sales Rep name"], how="left")
        for month in range(1, 13):
            month_name = pd.to_datetime(f"{month}", format="%m").strftime("%B")
            month_data = objectives[objectives["Month"] == month].rename(columns={"Objective": month_name})
            full_df = full_df.merge(
                month_data[["Product line", "Sales Rep name", month_name]], 
                on=["Product line", "Sales Rep name"], 
                how="left"
            )
        # Calculate the Annual Objective (if all month columns are missing, this will yield 0)
        full_df["Annual Objective"] = full_df.loc[:, "January":"December"].sum(axis=1, skipna=True)

        # Format numeric columns with $ symbol
        numeric_columns = [
            "Commission tier threshold", "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December", "Annual Objective"
        ]
        for col in numeric_columns:
            if col in full_df.columns:
                full_df[col] = full_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

        # Sort by Product line
        full_df = full_df.sort_values(by=["Product line", "Sales Rep name"]).reset_index(drop=True)

        # Add Sub-Total Rows
        rows_with_subtotals = []
        for product_line, group in full_df.groupby("Product line"):
            rows_with_subtotals.append(group)
            # Calculate sub-totals for numeric columns
            subtotal = group.loc[:, "January":"December"].applymap(
                lambda x: float(x.replace('$', '').replace(',', '')) if isinstance(x, str) else 0
            ).sum()
            subtotal["Annual Objective"] = subtotal.sum()
            subtotal_row = pd.Series({
                "Product line": product_line,
                "Sales Rep name": "Sub-Total",
                **{col: f"${subtotal[col]:,.2f}" for col in subtotal.index}
            })
            rows_with_subtotals.append(pd.DataFrame([subtotal_row]))

        final_df = pd.concat(rows_with_subtotals, ignore_index=True)
        return final_df

    except Exception as e:
        st.error(f"Error fetching business objective data: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()


def update_business_objective_data(df, year):
    """Update the sales_rep_business_objective and sales_rep_commission_tier_threshold tables."""
    engine = get_db_connection()
    try:
        # Filter out "Sub-Total" rows
        filtered_df = df[df["Sales Rep name"] != "Sub-Total"].copy()

        # Ensure no duplicates
        filtered_df = filtered_df.drop_duplicates()

        with engine.connect() as conn:
            with conn.begin():
                # Delete existing data for the year
                conn.execute(text("DELETE FROM sales_rep_business_objective WHERE \"Year\" = :year"), {"year": year})
                conn.execute(text("DELETE FROM sales_rep_commission_tier_threshold WHERE \"Year\" = :year"), {"year": year})

                # Prepare and insert sales_rep_business_objective data
                monthly_data = filtered_df.melt(
                    id_vars=["Product line", "Sales Rep name"], 
                    value_vars=["January", "February", "March", "April", "May", "June", 
                                "July", "August", "September", "October", "November", "December"],
                    var_name="Month", 
                    value_name="Objective"
                )
                monthly_data["Year"] = year
                monthly_data["Month"] = monthly_data["Month"].apply(lambda x: pd.to_datetime(x, format="%B").month)

                # Remove rows with null or invalid objectives
                monthly_data = monthly_data[~monthly_data["Objective"].isnull()]
                monthly_data["Objective"] = monthly_data["Objective"].apply(
                    lambda x: float(str(x).replace("$", "").replace(",", "").strip())
                )
                monthly_data.to_sql("sales_rep_business_objective", con=engine, if_exists="append", index=False)

                # Prepare and insert sales_rep_commission_tier_threshold data
                commission_data = filtered_df[["Product line", "Sales Rep name", "Commission tier threshold"]].copy()
                commission_data["Year"] = year
                commission_data["Commission tier threshold"] = commission_data["Commission tier threshold"].apply(
                    lambda x: float(str(x).replace("$", "").replace(",", "").strip())
                )
                commission_data.to_sql("sales_rep_commission_tier_threshold", con=engine, if_exists="append", index=False)

        st.success("Business objectives successfully updated!")
    except Exception as e:
        st.error(f"Error updating business objectives: {e}")
    finally:
        engine.dispose()

def highlight_subtotals_readonly(row):
    """Style rows where 'Sales Rep name' is 'Sub-Total'."""
    if row["Sales Rep name"] == "Sub-Total":
        return ["color: blue; font-weight: bold;" for _ in row]
    return [""] * len(row)

def remove_subtotals_for_editing(df):
    """Remove Sub-Total rows before making the DataFrame editable."""
    return df[df["Sales Rep name"] != "Sub-Total"].reset_index(drop=True)

# New helper function to fetch unique Sales Rep names from the sales_rep_commission_tier table.
def get_unique_sales_reps_commission_tier():
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
        st.error(f"Error fetching Sales Reps from commission tier: {e}")
        return []
    finally:
        engine.dispose()

def get_unique_product_lines_service_to_product():
    """Fetch distinct Product Lines from the service_to_product table."""
    query = """
        SELECT DISTINCT "Product Lines"
        FROM service_to_product
        WHERE "Product Lines" IS NOT NULL
        ORDER BY "Product Lines"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            product_lines = [row[0] for row in result.fetchall()]
        return product_lines
    except Exception as e:
        st.error(f"Error fetching unique product lines from service_to_product: {e}")
        return []
    finally:
        engine.dispose()


# ----------------- Streamlit UI -----------------
st.title("Business Objective Editor")
col1, col2 = st.columns([1, 1])

# Hardcoded year
selected_year = "2025"
with col1:
    st.markdown(f"**Year:** {selected_year}")

df = fetch_business_objective_data(selected_year)

# Initialize editing state if not already set.
if "editing" not in st.session_state:
    st.session_state.editing = False

if not st.session_state.editing:
    if df.empty:
        st.warning(f"No data available for the selected year: {selected_year}.")
    else:
        # Display read-only preview with sub-totals if data exists
        styled_df = df.style.apply(highlight_subtotals_readonly, axis=1)
        st.write("Preview with Sub-Totals Highlighted (Read-Only):")
        st.write(styled_df, unsafe_allow_html=True, use_container_width=True)
    
    if st.button("Edit Data"):
        st.session_state.editing = True
else:
    # Editing mode: remove Sub-Total rows.
    editable_df = remove_subtotals_for_editing(df)

    # Fetch unique Sales Rep names for the drop-down.
    sales_rep_options = get_unique_sales_reps_commission_tier()
    product_line_options = get_unique_product_lines_service_to_product()

    col_config = {
        "Sales Rep name": st.column_config.SelectboxColumn(
            "Sales Rep name",
            options=sales_rep_options,
            help="Select a Sales Rep name from the list"
        ),
        "Product line": st.column_config.SelectboxColumn(
            "Product line",
            options=product_line_options,
            help="Select a Product line from the list"
        )
    }

    st.write("Editable DataFrame (Sub-Totals Removed):")
    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="business_objective_editor",
        column_config=col_config
    )

    # Save changes with confirmation logic.
    if "save_initiated" not in st.session_state:
        st.session_state.save_initiated = False

    if st.button("Save Changes"):
        st.session_state.save_initiated = True
        st.warning("Are you sure you want to replace the current data with the new changes?")

    if st.session_state.save_initiated:
        if st.button("Yes, Replace Table"):
            update_business_objective_data(edited_df, selected_year)
            st.session_state.save_initiated = False  # Reset state after save
            st.session_state.editing = False  # Return to read-only mode
            st.rerun()

    if st.button("Cancel Editing"):
        st.session_state.editing = False
        st.rerun()

    else:
        st.warning(f"No data available for the selected year: {selected_year}.")
