import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_unique_years():
    """Fetch distinct years from the sales_rep_business_objective table."""
    query = """
        SELECT DISTINCT "Year"
        FROM sales_rep_business_objective
        ORDER BY "Year" DESC
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

def get_salespeople_by_year(selected_year):
    """Fetch distinct salespeople from harmonised_table filtered by year.
    Uses Revenue Recognition YYYY for sales objectives."""
    query = """
        SELECT DISTINCT "Sales Rep"
        FROM harmonised_table
        WHERE CAST("Revenue Recognition YYYY" AS INTEGER) = :year
        ORDER BY "Sales Rep"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"year": str(selected_year)})
            salespeople = [row[0] for row in result.fetchall()]
        return salespeople
    except Exception as e:
        st.error(f"Error fetching salespeople: {e}")
        return []
    finally:
        engine.dispose()

def get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson):
    """Fetch distinct product lines from harmonised_table filtered by year and salesperson.
    Uses Revenue Recognition YYYY for sales objectives."""
    query = """
        SELECT DISTINCT LOWER("Product Line")
        FROM harmonised_table
        WHERE CAST("Revenue Recognition YYYY" AS INTEGER) = :year
          {salesperson_filter}
        ORDER BY LOWER("Product Line")
    """
    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            
            # Get the lowercase product lines from the database
            lowercase_product_lines = [row[0] for row in result.fetchall()]
            
            # Transform to title case for display
            display_product_lines = [pl.title() for pl in lowercase_product_lines]
            
            # Store mapping between display values and lowercase values
            st.session_state.product_line_map = {
                display: lower for display, lower in zip(display_product_lines, lowercase_product_lines)
            }
            
        return display_product_lines
    except Exception as e:
        st.error(f"Error fetching product lines: {e}")
        return []
    finally:
        engine.dispose()

def get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson):
    """Fetch YTD sales actual from the harmonised_table.
    Uses Revenue Recognition YYYY for sales objectives."""
    query = """
        SELECT SUM("Sales Actual") AS ytd_sales_actual
        FROM harmonised_table
        WHERE CAST("Revenue Recognition YYYY" AS INTEGER) = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND LOWER("Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""

    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": selected_year}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_sales_actual = result.scalar() or 0
        return ytd_sales_actual
    except Exception as e:
        st.error(f"Error fetching YTD Sales Actual: {e}")
        return 0
    finally:
        engine.dispose()

def get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson):
    """Fetch YTD revenue actual from the harmonised_table.
    Uses Revenue Recognition YYYY for sales objectives."""
    query = """
        SELECT SUM("Rev Actual") AS ytd_revenue_actual
        FROM harmonised_table
        WHERE CAST("Revenue Recognition YYYY" AS INTEGER) = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND LOWER("Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""

    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": selected_year}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_revenue_actual = result.scalar() or 0
        return ytd_revenue_actual
    except Exception as e:
        st.error(f"Error fetching YTD Revenue Actual: {e}")
        return 0
    finally:
        engine.dispose()

def get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson):
    """Fetch the YTD SHS Margin from the harmonised_table.
    Uses Revenue Recognition YYYY for sales objectives."""
    query = """
        SELECT SUM("SHS Margin") AS ytd_shs_margin
        FROM harmonised_table
        WHERE "Revenue Recognition YYYY" = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND LOWER("Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""

    salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_shs_margin = result.scalar()
        return ytd_shs_margin if ytd_shs_margin else 0.0
    except Exception as e:
        st.error(f"Error fetching YTD SHS Margin: {e}")
        return 0.0
    finally:
        engine.dispose()

def get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson):
    """Fetch the YTD Commission Payout dynamically.
    Uses Commission Date YYYY for commission attribution."""
    query = """
        SELECT 
            SUM(
                CASE 
                    WHEN "Commission tier 2 date" IS NULL 
                    THEN "Comm Amount tier 1"
                    WHEN SPLIT_PART("Commission tier 2 date", '-', 2)::INTEGER < "Commission Date MM"::INTEGER
                    THEN "Comm Amount tier 1"
                    ELSE "Comm Amount tier 1" + "Comm tier 2 diff amount"
                END
            ) AS ytd_commission_payout
        FROM harmonised_table
        WHERE "Commission Date YYYY" = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND LOWER("Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""

    salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_commission_payout = result.scalar() or 0.0
        return ytd_commission_payout
    except Exception as e:
        st.error(f"Error fetching YTD Commission Payout: {e}")
        return 0.0
    finally:
        engine.dispose()

def fetch_objectives(selected_year, selected_product_line, selected_salesperson):
    """
    Fetch the monthly Sales Objective from the sales_rep_business_objective table,
    applying filters based on the user's selection.
    """
    query = """
    SELECT "Month", SUM("Objective") as "Sales Objective"
    FROM sales_rep_business_objective
    WHERE "Year" = :year
    """
    params = {"year": selected_year}
    if selected_salesperson != "All":
        query += ' AND "Sales Rep name" = :sales_rep'
        params["sales_rep"] = selected_salesperson
    if selected_product_line != "All":
        query += ' AND LOWER("Product line") = LOWER(:product_line)'
        params["product_line"] = selected_product_line
    query += ' GROUP BY "Month" ORDER BY "Month"::integer'
    
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params=params)
        df["Month"] = df["Month"].astype(int)
        df = df.rename(columns={"Month": "month_number"})
        return df
    except Exception as e:
        st.error(f"Error fetching objectives: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def fetch_monthly_data(selected_year, selected_product_line, selected_salesperson):
    """
    Fetch monthly sales performance data from harmonised_table with proper handling of:
    - Sales Actual/Revenue Actual based on Revenue Recognition dates
    - Commission Payout based on Commission dates
    
    These two date types may not align (e.g., revenue recognized in August but commission paid in December).
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # Decide on extra grouping keys based on selections.
            select_extra_rev = ""
            group_by_extra_rev = ""
            select_extra_comm = ""
            group_by_extra_comm = ""
            
            if selected_product_line == "All" and selected_salesperson == "All":
                # Case 1: multiple reps × multiple lines
                # — for sales, we only care about month
                select_extra_rev    = ''
                group_by_extra_rev  = ''
                # — for commission, keep rep+line so thresholds apply per rep/line
                select_extra_comm   = ', h."Sales Rep" AS "Sales Rep", h."Product Line" AS "Product Line"'
                group_by_extra_comm = ', h."Sales Rep", h."Product Line"'
            elif selected_product_line == "All" and selected_salesperson != "All":
                # Case 2: Single salesperson, multiple product lines.
                select_extra_rev = ', h."Product Line" AS "Product Line"'
                group_by_extra_rev = ', h."Product Line"'
                select_extra_comm = ', h."Product Line" AS "Product Line"'
                group_by_extra_comm = ', h."Product Line"'

            elif selected_product_line != "All" and selected_salesperson == "All":
                # Case 3: single product line, multiple sales reps
                select_extra_rev = ', h."Sales Rep" AS "Sales Rep"'
                group_by_extra_rev = ', h."Sales Rep"'
                # select_extra_comm = ', h."Sales Rep" AS "Sales Rep"'
                # group_by_extra_comm = ', h."Sales Rep"'
                select_extra_comm   = ', h."Sales Rep" AS "Sales Rep"'
                group_by_extra_comm = ', h."Sales Rep"'
            # Else, Case 4: Both filters fixed; no extra grouping needed.
            else:
                # Case 4: single rep × single line → aggregate only by month
                select_extra_rev    = ''
                group_by_extra_rev  = ''
                # but still keep per-rep/line in commission so tier logic works
                select_extra_comm   = ', h."Sales Rep" AS "Sales Rep", h."Product Line" AS "Product Line"'
                group_by_extra_comm = ', h."Sales Rep", h."Product Line"'
            
            # FIRST QUERY: Get sales data grouped by Revenue Recognition Month
            sales_query = f"""
                SELECT 
                    h."Revenue Recognition MM"::INTEGER AS month_number
                    {select_extra_rev},
                    SUM(h."Sales Actual") AS "Sales Actual",
                    SUM(h."Rev Actual") AS "Revenue Actual"
                FROM harmonised_table h
                WHERE h."Revenue Recognition YYYY" = :year
                  {f'AND LOWER(h."Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""}
                  {f'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""}
                GROUP BY h."Revenue Recognition MM" {group_by_extra_rev}
                ORDER BY h."Revenue Recognition MM"
            """
            sales_params = {"year": str(selected_year)}
            if selected_product_line != "All":
                sales_params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                sales_params["salesperson"] = selected_salesperson

            sales_result = conn.execute(text(sales_query), sales_params)
            sales_data = pd.DataFrame(sales_result.fetchall(), columns=sales_result.keys())
            
            # SECOND QUERY: Get commission data grouped by Commission Date Month
            commission_query = f"""
                SELECT 
                    h."Commission Date MM"::INTEGER AS commission_month
                    {select_extra_comm},
                    SUM(h."Comm Amount tier 1") AS tier1_sum,
                    SUM(h."Comm tier 2 diff amount") AS tier2_sum,
                    MAX(h."Commission tier 2 date") AS tier2_date,
                    SUM(h."Sales Actual") AS month_sales
                FROM harmonised_table h
                WHERE h."Commission Date YYYY" = :year
                  {f'AND LOWER(h."Product Line") = LOWER(:product_line)' if selected_product_line != "All" else ""}
                  {f'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""}
                GROUP BY h."Commission Date MM" {group_by_extra_comm}
                ORDER BY h."Commission Date MM"::INTEGER
            """
            
            commission_result = conn.execute(text(commission_query), sales_params)
            commission_data = pd.DataFrame(commission_result.fetchall(), columns=commission_result.keys())
            
            # Create a full months dataframe for display
            all_months_df = pd.DataFrame({"month_number": list(range(1, 13))})
            all_months_df["Month"] = all_months_df["month_number"].apply(
                lambda x: pd.to_datetime(str(x), format="%m").strftime("%B")
            )
            
            # Merge sales data with all months
            if not sales_data.empty:
                sales_data_merged = pd.merge(all_months_df, sales_data, on="month_number", how="left").fillna(0)
                for col in ["Sales Actual", "Revenue Actual"]:
                    if col in sales_data_merged.columns:
                        sales_data_merged[col] = sales_data_merged[col].astype(float)
            else:
                sales_data_merged = all_months_df.copy()
                sales_data_merged["Sales Actual"] = 0.0
                sales_data_merged["Revenue Actual"] = 0.0
            
            # Process commission data with threshold logic
            if not commission_data.empty:
                # Set up commission DataFrame with proper grouping
                commission_cols = ["commission_month"]
                if "Sales Rep" in commission_data.columns:
                    commission_cols.append("Sales Rep")
                if "Product Line" in commission_data.columns:
                    commission_cols.append("Product Line")
                
                # List to store processed commission data by group
                processed_commission_list = []
                
                # Function to process a group's commission data
                def process_commission_group(group_df, sales_rep, product_line_val):
                    # Get threshold from sales_rep_commission_tier_threshold
                    threshold_query = """
                        SELECT "Commission tier threshold"
                        FROM sales_rep_commission_tier_threshold
                        WHERE lower("Sales Rep name") = lower(:sales_rep)
                        AND "Year" = :year
                        AND lower("Product line") = lower(:product_line)
                    """
                    
                    threshold_result = conn.execute(
                        text(threshold_query),
                        {"sales_rep": sales_rep, "year": selected_year, "product_line": product_line_val}
                    )
                    threshold = threshold_result.scalar()
                    if threshold is None:
                        threshold = float('inf')  # If no threshold, use infinity
                    
                    # Sort by month to calculate cumulative sales
                    sorted_df = group_df.sort_values("commission_month")
                    
                    # Calculate cumulative sales and commission
                    cumulative_sales = 0
                    deferred_tier2 = 0
                    threshold_reached = False
                    commission_dict = {}
                    
                    for _, row in sorted_df.iterrows():
                        month = int(row["commission_month"])
                        month_sales = float(row["month_sales"]) if pd.notnull(row["month_sales"]) else 0
                        tier1 = float(row["tier1_sum"]) if pd.notnull(row["tier1_sum"]) else 0
                        tier2 = float(row["tier2_sum"]) if pd.notnull(row["tier2_sum"]) else 0
                        
                        cumulative_sales += month_sales
                        
                        if not threshold_reached:
                            if cumulative_sales < threshold:
                                # Not reached threshold yet - pay tier1 only
                                commission = tier1
                                deferred_tier2 += tier2
                            else:
                                # Just reached threshold - pay everything
                                threshold_reached = True
                                commission = tier1 + tier2 + deferred_tier2
                                deferred_tier2 = 0
                        else:
                            # Already above threshold - pay both tiers
                            commission = tier1 + tier2
                        
                        commission_dict[month] = commission
                    
                    # Create a DataFrame for all months with this group's commission
                    result_df = pd.DataFrame({"month_number": list(range(1, 13))})
                    result_df["Commission Payout"] = result_df["month_number"].map(
                        lambda m: commission_dict.get(m, 0.0)
                    )
                    
                    # Add grouping columns back if needed
                    if "Sales Rep" in group_df.columns:
                        result_df["Sales Rep"] = sales_rep
                    if "Product Line" in group_df.columns:
                        result_df["Product Line"] = product_line_val
                        
                    return result_df
                
                # Process each commission group
                if "Sales Rep" in commission_data.columns and "Product Line" in commission_data.columns:
                    # Group by both Sales Rep and Product Line
                    for (sr, pl), group in commission_data.groupby(["Sales Rep", "Product Line"]):
                        processed_group = process_commission_group(group, sr, pl)
                        processed_commission_list.append(processed_group)
                    
                elif "Sales Rep" in commission_data.columns:
                    # Group by Sales Rep only
                    for sr, group in commission_data.groupby("Sales Rep"):
                        processed_group = process_commission_group(group, sr, selected_product_line)
                        processed_commission_list.append(processed_group) 
                    
                elif "Product Line" in commission_data.columns:
                    # Group by Product Line only
                    for pl, group in commission_data.groupby("Product Line"):
                        processed_group = process_commission_group(group, selected_salesperson, pl)
                        processed_commission_list.append(processed_group)
                    
                else:
                    # No grouping needed
                    processed_group = process_commission_group(
                        commission_data, 
                        selected_salesperson, 
                        selected_product_line
                    )
                    processed_commission_list.append(processed_group)
                
                # Combine all processed groups
                if processed_commission_list:
                    all_commission_data = pd.concat(processed_commission_list, ignore_index=True)
                    
                    # Aggregate by month and any grouping columns
                    group_cols = ["month_number"]
                    if "Sales Rep" in all_commission_data.columns:
                        group_cols.append("Sales Rep")
                    if "Product Line" in all_commission_data.columns:
                        group_cols.append("Product Line")
                    
                    commission_data_for_merge = all_commission_data.groupby(
                        group_cols, as_index=False
                    ).agg({"Commission Payout": "sum"})
                else:
                    # Create empty DataFrame with right structure
                    commission_data_for_merge = pd.DataFrame({
                        "month_number": list(range(1, 13)),
                        "Commission Payout": 0.0
                    })
            else:
                # Create empty commission data with zeros
                commission_data_for_merge = pd.DataFrame({
                    "month_number": list(range(1, 13)),
                    "Commission Payout": 0.0
                })
            
            # Merge sales and commission data
            comm_by_month = (
                commission_data_for_merge
                .groupby("month_number", as_index=False)["Commission Payout"]
                .sum()
            )

            merged_df = (
                pd.merge(
                    sales_data_merged,
                    comm_by_month,
                    on="month_number",
                    how="left"
                )
                .fillna(0)
            )
            
            # Calculate SHS Margin
            merged_df["SHS Margin"] = merged_df["Revenue Actual"] - merged_df["Commission Payout"]
            
            # Merge with Sales Objectives
            objectives_df = fetch_objectives(selected_year, selected_product_line, selected_salesperson)
            if not objectives_df.empty:
                merged_df = pd.merge(merged_df, objectives_df, on="month_number", how="left").fillna(0)
                merged_df["% to Objective"] = merged_df.apply(
                    lambda row: f"{(row['Sales Actual'] / row['Sales Objective'] * 100):.2f}%"
                    if row["Sales Objective"] > 0 else "0.00%", axis=1
                )
            else:
                merged_df["Sales Objective"] = 0
                merged_df["% to Objective"] = "0.00%"
            
            return merged_df

    except Exception as e:
        st.error(f"Error fetching monthly data: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()


def get_years_for_sales_rep_any():
    """Fetch distinct years for all Sales Reps from the harmonised_table."""
    query = """
        SELECT DISTINCT "Revenue Recognition YYYY"
        FROM harmonised_table
        ORDER BY "Revenue Recognition YYYY"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            years = [row[0] for row in result.fetchall()]
        return years
    except Exception as e:
        st.error(f"Error fetching years for all Sales Reps: {e}")
        return []
    finally:
        engine.dispose()

def render_preview_table(df, css_class="", drop_index=True):
    """
    Render a DataFrame as an HTML table using custom CSS.
    If drop_index is True, the index is reset and not shown;
    otherwise the index is preserved.
    """
    if drop_index:
        html_table = df.reset_index(drop=True).to_html(index=False, classes=css_class)
    else:
        html_table = df.to_html(index=True, classes=css_class)
    st.markdown(html_table, unsafe_allow_html=True)

# ----------------- Streamlit UI -----------------

st.title("Sales Performance")

col1, col2 = st.columns([1, 1])

with col1:
    years = get_unique_years()
    if not years:
        st.warning("No years available.")
    else:
        selected_year = st.selectbox("Select a Year:", years)
        if selected_year:
            salespeople = get_salespeople_by_year(selected_year)
            # Apply restriction for simple users:
            if "user_permission" in st.session_state and st.session_state.user_permission.lower() == "user":
                user_name = st.session_state.user_name
                if user_name in salespeople:
                    salespeople = ["All", user_name]
                else:
                    salespeople = ["All"]
            else:
                salespeople.insert(0, "All")
            
            selected_salesperson = st.selectbox("Choose a Salesperson:", salespeople)
            product_lines = get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson)
            display_product_lines = ["All"] + product_lines  # Add "All" option at the beginning

            selected_product_line_display = st.selectbox("Choose a Product Line:", display_product_lines)

            # Convert the display value back to lowercase for backend processing
            if selected_product_line_display != "All":
                selected_product_line = st.session_state.product_line_map.get(selected_product_line_display, selected_product_line_display.lower())
            else:
                selected_product_line = "All"


monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)
if monthly_data is None or monthly_data.empty:
    monthly_data = pd.DataFrame()

with col2:
    if not monthly_data.empty:
        def parse_currency(value):
            try:
                return float(str(value).replace("$", "").replace(",", "").replace("%", ""))
            except ValueError:
                return 0.0
        numeric_columns = ["Sales Actual", "Revenue Actual", "SHS Margin", "Commission Payout"]
        for col in numeric_columns:
            if col in monthly_data.columns:
                monthly_data[col] = monthly_data[col].apply(parse_currency)
        ytd_sales_actual = monthly_data["Sales Actual"].sum() if "Sales Actual" in monthly_data.columns else 0.0
        ytd_revenue_actual = monthly_data["Revenue Actual"].sum() if "Revenue Actual" in monthly_data.columns else 0.0
        ytd_shs_margin = monthly_data["SHS Margin"].sum() if "SHS Margin" in monthly_data.columns else 0.0
        ytd_commission_payout = monthly_data["Commission Payout"].sum() if "Commission Payout" in monthly_data.columns else 0.0

        st.markdown(
            f"""
            <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
                YTD Sales Actual: ${ytd_sales_actual:,.2f}<br/>
                YTD Revenue Actual: ${ytd_revenue_actual:,.2f}<br/>
                YTD SHS Margin: ${ytd_shs_margin:,.2f}<br/>
                YTD Commission Payout: ${ytd_commission_payout:,.2f}
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.warning("No data available for the selected filters.")

monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

if not monthly_data.empty:
    monthly_data = monthly_data.set_index("Month").T
    row_order = ["Sales Actual", "Sales Objective", "% to Objective", 
                 "Revenue Actual", "Commission Payout", "SHS Margin"]
    monthly_data = monthly_data.reindex(row_order)
    monthly_data = monthly_data.loc[:, ~monthly_data.columns.duplicated()]
    all_months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    sales_actual = [float(str(monthly_data.loc["Sales Actual", m]).replace("$", "").replace(",", ""))
                    if m in monthly_data.columns else 0 for m in all_months]
    sales_objective = [float(str(monthly_data.loc["Sales Objective", m]).replace("$", "").replace(",", ""))
                    if m in monthly_data.columns else 0 for m in all_months]
    sales_actual = [x if x > 0 else 0 for x in sales_actual]
    fig, ax = plt.subplots(figsize=(12, 3))
    ax.bar(all_months, sales_actual, label="Sales Actual", color="blue", alpha=0.7)
    ax.bar(all_months, sales_objective, label="Sales Objective", color="orange", alpha=0.7, width=0.4, align="edge")
    ax.set_title("Sales vs Sales Objective", fontsize=16, fontweight="bold")
    ax.set_xlabel("Months", fontsize=12)
    ax.set_ylabel("Sales ($)", fontsize=12)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xticklabels(all_months, rotation=45, ha="right", fontsize=9)
    st.pyplot(fig)

    for col in monthly_data.columns:
        monthly_data[col] = monthly_data[col].apply(
            lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) and not str(x).endswith('%') else x
        )

    st.subheader("Monthly Performance Summary")
    st.markdown(
        """
        <style>
        .large_table {
            width: 100%;
            font-size: 16px;
        }
        .large_table th, .large_table td {
            text-align: right !important;
        }
        .large_table th:nth-child(1),
        .large_table td:nth-child(1) {
            width: 200px !important;
            text-align: left !important
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    render_preview_table(monthly_data, css_class="large_table", drop_index=False)
    
    def fetch_data_status():
        query = "SELECT * FROM data_status"
        engine = get_db_connection()
        try:
            with engine.connect() as conn:
                data_status = pd.read_sql_query(query, conn)
            return data_status
        except Exception as e:
            st.error(f"Error fetching data status table: {e}")
            return pd.DataFrame()
        finally:
            engine.dispose()
            
    data_status_df = fetch_data_status().sort_values(by="Product line", ascending=True)
    if not data_status_df.empty:
        st.subheader("Data Upload Status")
        boolean_columns = [col for col in data_status_df.columns if col != "Product line"]
        
        for col in boolean_columns:
            data_status_df[col] = data_status_df[col].fillna(False).astype(bool)
            data_status_df[col] = data_status_df[col].map(
                lambda x: '<span style="font-size:16px; color:green; display:block; text-align:center;">&#10004;</span>'
                        if x 
                        else '<span style="font-size:16px; color:red; display:block; text-align:center;">&#10006;</span>'
            )
        data_status_df["Product line"] = data_status_df["Product line"].astype(str)
        
        st.markdown(
            """
            <style>
            .large_table {
                width: 100%;
                font-size: 16px;
                table-layout: fixed;
            }
            .large_table th {
                text-align: center !important;
                padding: 4px 8px;
                word-wrap: break-word;
            }
            .large_table td {
                text-align: right !important;
                padding: 4px 8px;
                word-wrap: break-word;
            }
            .large_table th:nth-child(1),
            .large_table td:nth-child(1) {
                width: 200px !important;
                text-align: left !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        html_table = data_status_df.reset_index(drop=True).to_html(index=False, classes="large_table", escape=False)
        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.warning("No data available in the data_status table.")