import os
import hashlib
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def generate_row_hash(row: pd.Series) -> str:
    """
    Generate a hash for identifying unique rows based on critical columns.
    (For QuickBooks these include Revenue Recognition Date, Product Lines, Service Lines, Customer and Company name.)
    """
    columns_to_hash = [
        "Revenue Recognition Date", "Product Lines", "Service Lines", "Customer", "Sales Rep Name"
    ]
    row_data = ''.join([str(row[col]) for col in columns_to_hash if col in row]).encode('utf-8')
    return hashlib.sha256(row_data).hexdigest()

def save_dataframe_to_db(df: pd.DataFrame, table_name: str = "master_quickbooks_sales"):
    """
    Save data to the master_quickbooks_sales table by removing entries based on 'Revenue Recognition Date YYYY' and 'Revenue Recognition Date MM'.
    Then, update the harmonised_table by mapping the new data and recalculating commission tier 2 dates.
    
    Returns a list of debug messages.
    """
    engine = get_db_connection()
    debug_messages = []

    # Generate row_hash for each row
    df["row_hash"] = df.apply(generate_row_hash, axis=1)
    
    try:
        with engine.connect() as conn:
            # Find which Revenue Recognition column names are used in this DataFrame
            rev_year_col = None
            rev_month_col = None
            
            # Check for different variations of column names
            for col in ["Revenue Recognition Date YYYY", "Revenue Recognition YYYY"]:
                if col in df.columns:
                    rev_year_col = col
                    break
            
            for col in ["Revenue Recognition Date MM", "Revenue Recognition MM"]:
                if col in df.columns:
                    rev_month_col = col
                    break
            
            if not rev_year_col or not rev_month_col:
                # Handle missing Revenue Recognition columns - fallback to Commission Date
                debug_messages.append("⚠️ Warning: Could not find Revenue Recognition Date year/month columns in the DataFrame.")
                
                # Check if there are Commission Date columns to use as fallback
                if "Commission Date YYYY" in df.columns and "Commission Date MM" in df.columns:
                    date_values = df[['Commission Date YYYY', 'Commission Date MM']].drop_duplicates().values.tolist()
                    
                    # Convert the date_values into a filterable SQL condition
                    condition = " OR ".join(
                        [f'("Commission Date YYYY" = \'{yyyy}\' AND "Commission Date MM" = \'{mm}\')' 
                         for yyyy, mm in date_values]
                    )
                    
                    debug_messages.append("⚠️ Using Commission Date columns as fallback for deletion criteria.")
                else:
                    # No date columns found - this may result in unintended behavior
                    debug_messages.append("❌ Error: No valid date columns found for deletion criteria.")
                    debug_messages.append("⚠️ Falling back to Product Line-based deletion to maintain data integrity.")
                    
                    # For QuickBooks, we can safely delete and replace by Data Source in harmonised_table
                    # But for master_quickbooks_sales, we need an approach that won't delete everything
                    
                    # Get unique product lines from the new data
                    if "Product Lines" in df.columns:
                        product_lines = df["Product Lines"].unique().tolist()
                        if product_lines:
                            product_line_conditions = " OR ".join([f'"Product Lines" = \'{pl}\'' for pl in product_lines])
                            condition = f"({product_line_conditions})"
                        else:
                            condition = "1=0"  # Don't delete anything if no product lines found
                    else:
                        condition = "1=0"  # Don't delete anything if no product lines column
            else:
                # Use Revenue Recognition Date columns as intended
                date_values = df[[rev_year_col, rev_month_col]].drop_duplicates().values.tolist()
                
                # For database, we need to check which column names exist in the table
                inspector = inspect(engine)
                table_columns = [c["name"] for c in inspector.get_columns(table_name)]
                
                # Find the matching column names in the database table
                db_rev_year_col = None
                db_rev_month_col = None
                
                for col in table_columns:
                    # For year column
                    if col in ["Revenue Recognition Date YYYY", "Revenue Recognition YYYY"]:
                        db_rev_year_col = col
                    # For month column
                    if col in ["Revenue Recognition Date MM", "Revenue Recognition MM"]:
                        db_rev_month_col = col
                
                if not db_rev_year_col or not db_rev_month_col:
                    debug_messages.append(f"⚠️ Warning: Revenue Recognition Date columns not found in table {table_name}.")
                    # Fallback to Commission Date columns in the database
                    if "Commission Date YYYY" in table_columns and "Commission Date MM" in table_columns:
                        condition = " OR ".join(
                            [f'("Commission Date YYYY" = \'{yyyy}\' AND "Commission Date MM" = \'{mm}\')' 
                             for yyyy, mm in date_values]
                        )
                        debug_messages.append("⚠️ Using Commission Date columns in database as fallback for deletion.")
                    else:
                        # No date columns found - this may result in unintended behavior
                        debug_messages.append("❌ Error: No valid date columns found in database for deletion.")
                        debug_messages.append("⚠️ Falling back to Product Line-based deletion to maintain data integrity.")
                        
                        # For QuickBooks with dynamic product lines, we can try to delete by product line
                        product_lines_col = "Product Lines" if "Product Lines" in table_columns else None
                        if product_lines_col and "Product Lines" in df.columns:
                            product_lines = df["Product Lines"].unique().tolist()
                            if product_lines:
                                product_line_conditions = " OR ".join([f'"{product_lines_col}" = \'{pl}\'' for pl in product_lines])
                                condition = f"({product_line_conditions})"
                            else:
                                condition = "1=0"  # Don't delete anything if no product lines found
                        else:
                            condition = "1=0"  # Don't delete anything if no product lines column
                else:
                    # Build the condition using the actual column names from the database
                    condition = " OR ".join(
                        [f'("{db_rev_year_col}" = \'{yyyy}\' AND "{db_rev_month_col}" = \'{mm}\')' 
                         for yyyy, mm in date_values]
                    )
                    debug_messages.append(f"✅ Using Revenue Recognition Date columns for deletion criteria ({len(date_values)} date combinations).")
            
            # Special handling for QuickBooks - combine product lines with date condition if both are available
            if "Product Lines" in df.columns and len(df["Product Lines"].unique()) > 1:
                debug_messages.append(f"📊 QuickBooks has {len(df['Product Lines'].unique())} unique product lines.")
                
                # If we're deleting by dates, it's safer to also consider product lines for QuickBooks
                # This prevents deleting data from unrelated product lines that happen to have the same dates
                if condition != "1=0":
                    product_lines = df["Product Lines"].unique().tolist()
                    product_line_conditions = " OR ".join([f'"Product Lines" = \'{pl}\'' for pl in product_lines])
                    condition = f"({condition}) AND ({product_line_conditions})"
                    debug_messages.append("✅ Enhanced deletion criteria with Product Lines for safety.")

            # Delete existing records matching the specified condition
            delete_query = text(f"DELETE FROM {table_name} WHERE {condition}")
            result = conn.execute(delete_query)
            conn.commit()
            
            # Log how many records were deleted
            debug_messages.append(f"✅ Deleted {result.rowcount} records from '{table_name}' matching the specified criteria.")

            # Append the dataframe to the table
            df.to_sql(table_name, con=engine, if_exists="append", index=False)
            debug_messages.append(f"✅ {len(df)} new records successfully added to '{table_name}'.")

            # Update harmonised_table with the newly mapped QuickBooks data
            harmonised_messages = update_harmonised_table(table_name)
            debug_messages.extend(harmonised_messages)
            
            # Now update the Commission tier 2 date for QuickBooks data.
            if table_name == "master_quickbooks_sales":
                commission_tier_2_messages = update_commission_tier_2_date()
                debug_messages.extend(commission_tier_2_messages)

    except SQLAlchemyError as e:
        error_message = str(e)
        print(f"❌ Error saving data to '{table_name}': {error_message}")
        debug_messages.append(f"❌ Error saving data to '{table_name}': {error_message}")
    finally:
        engine.dispose()

    return debug_messages

def update_harmonised_table(table_name: str):
    """
    Harmonise the specific table (for QuickBooks) and update the harmonised_table.
    
    Because QuickBooks rows have dynamic product lines, we first delete all rows in harmonised_table 
    that came from master_quickbooks_sales (using "Data Source" as the link), and then append the newly 
    mapped data.
    
    Returns a list of debug messages.
    """
    debug_messages = []
    
    if table_name == "master_quickbooks_sales":
        harmonised_data = map_quickbooks_to_harmonised()
        if harmonised_data is not None:
            engine = get_db_connection()
            try:
                with engine.connect() as conn:
                    # Define the data source value for QuickBooks ingestion
                    data_source = "master_quickbooks_sales"
                    
                    # Delete existing harmonised_table rows that came from QuickBooks using the Data Source
                    delete_query = text("""
                        DELETE FROM harmonised_table 
                        WHERE "Data Source" = :data_source
                    """)
                    conn.execute(delete_query, {"data_source": data_source})
                    conn.commit()
                    debug_messages.append("✅ Deleted existing harmonised_table rows corresponding to master_quickbooks_sales.")

                    # Append the newly harmonised data
                    harmonised_data.to_sql("harmonised_table", con=engine, if_exists="append", index=False)
                    debug_messages.append(f"✅ Harmonised table updated with new data for '{table_name}'.")
            except SQLAlchemyError as e:
                debug_messages.append(f"❌ Error updating harmonised_table: {e}")
            finally:
                engine.dispose()
        else:
            debug_messages.append(f"⚠️ No harmonised data available for '{table_name}'.")
    
    return debug_messages


def map_quickbooks_to_harmonised():
    """
    Map master_quickbooks_sales data to the harmonised_table structure.
    
    Equivalence between the two systems:
      - "Revenue Recognition Date" from QuickBooks is kept as "Revenue Recognition Date"
      - "Revenue Recognition Date MM" from QuickBooks is kept as "Revenue Recognition MM"
      - "Revenue Recognition Date YYYY" from QuickBooks is kept as "Revenue Recognition YYYY"
      - "Commission Date" from QuickBooks is kept as "Commission Date"
      - "Commission Date MM" from QuickBooks is kept as "Commission Date MM"
      - "Commission Date YYYY" from QuickBooks is kept as "Commission Date YYYY"
      - "Sales Rep Name" from QuickBooks is mapped to "Sales Rep"
      - "Product Lines" from QuickBooks becomes "Product Line"
      - "Amount line" from QuickBooks becomes "Sales Actual"
      - "Margin" from QuickBooks becomes "Rev Actual"
    
    Additional calculations for commission amounts and SHS Margin are performed.
    A literal column "Data Source" is added with the value 'master_quickbooks_sales'.
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # First get the max Revenue Recognition Date MM value to use in the cumulative_revenue CTE
            max_month_query = text("""
                SELECT MAX(CAST("Revenue Recognition Date MM" AS INTEGER)) FROM master_quickbooks_sales
            """)
            max_month_result = conn.execute(max_month_query).scalar() or 12  # Default to 12 if null
            
            # Now use the max_month value in the main query
            query = text("""
WITH commission_rates AS (
    SELECT 
        "Sales Rep Name", 
        "Commission tier 1 rate", 
        "Commission tier 2 rate"
    FROM sales_rep_commission_tier
),
tier_thresholds AS (
    SELECT 
        "Product line",
        "Sales Rep name",
        "Year",
        "Commission tier threshold"
    FROM sales_rep_commission_tier_threshold
),
cumulative_revenue AS (
    SELECT 
        "Sales Rep",
        "Revenue Recognition YYYY",
        "Product Line",
        SUM("Rev Actual") AS total_revenue_ytd
    FROM harmonised_table
    WHERE "Product Line" IN (SELECT DISTINCT "Product Lines" FROM master_quickbooks_sales) 
      AND CAST("Revenue Recognition MM" AS INTEGER) BETWEEN 1 AND :max_month 
    GROUP BY "Sales Rep", "Revenue Recognition YYYY", "Product Line"
),
commission_calculations AS (
    SELECT 
        mqs."Revenue Recognition Date",
        mqs."Revenue Recognition Date MM",
        mqs."Revenue Recognition Date YYYY",
        mqs."Commission Date",
        mqs."Commission Date MM",
        mqs."Commission Date YYYY",
        mqs."Sales Rep Name",
        mqs."Product Lines", 
        mqs."Amount line", 
        mqs."Margin",

        CAST((mqs."Margin" * crt."Commission tier 1 rate") AS NUMERIC(15,2)) AS "Comm Amount tier 1",

        CAST((mqs."Margin" * crt."Commission tier 2 rate" - mqs."Margin" * crt."Commission tier 1 rate") AS NUMERIC(15,2)) AS "Comm tier 2 diff amount",

        c.total_revenue_ytd,
        thr."Commission tier threshold",
        mqs.row_hash

    FROM master_quickbooks_sales AS mqs
    LEFT JOIN commission_rates AS crt 
        ON mqs."Sales Rep Name" = crt."Sales Rep Name"
    LEFT JOIN tier_thresholds AS thr
        ON mqs."Sales Rep Name" = thr."Sales Rep name"
        AND mqs."Product Lines" = thr."Product line"
        AND CAST(mqs."Revenue Recognition Date YYYY" AS INTEGER) = thr."Year"
    LEFT JOIN cumulative_revenue AS c
        ON mqs."Sales Rep Name" = c."Sales Rep"
        AND CAST(mqs."Revenue Recognition Date YYYY" AS INTEGER) = CAST(c."Revenue Recognition YYYY" AS INTEGER)
        AND mqs."Product Lines" = c."Product Line"  
),
tier_2_eligibility AS (
    SELECT 
        "Sales Rep Name",
        "Commission Date YYYY",
        "Product Lines",
        MIN("Commission Date") AS "Commission tier 2 date"
    FROM commission_calculations
    WHERE (total_revenue_ytd + "Margin") >= "Commission tier threshold"
    GROUP BY "Sales Rep Name", "Commission Date YYYY", "Product Lines"
)
SELECT 
    c."Commission Date" AS "Commission Date",
    c."Commission Date MM" AS "Commission Date MM",
    c."Commission Date YYYY" AS "Commission Date YYYY",
    c."Revenue Recognition Date" AS "Revenue Recognition Date",
    c."Revenue Recognition Date MM" AS "Revenue Recognition MM",
    c."Revenue Recognition Date YYYY" AS "Revenue Recognition YYYY",
    c."Sales Rep Name" AS "Sales Rep",
    c."Product Lines" AS "Product Line",
    c."Amount line" AS "Sales Actual",
    c."Margin" AS "Rev Actual",
    c."Comm Amount tier 1",
    c."Comm tier 2 diff amount",
    t2."Commission tier 2 date",
    'master_quickbooks_sales' AS "Data Source",
    c.row_hash
FROM commission_calculations AS c
LEFT JOIN tier_2_eligibility AS t2 
    ON c."Sales Rep Name" = t2."Sales Rep Name"
    AND CAST(c."Commission Date YYYY" AS INTEGER) = CAST(t2."Commission Date YYYY" AS INTEGER)
    AND c."Product Lines" = t2."Product Lines";
            """)
            
            # Execute with the max_month parameter
            result = conn.execute(query, {"max_month": max_month_result})
            harmonised_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return harmonised_df
    except SQLAlchemyError as e:
        print(f"❌ Error mapping master_quickbooks_sales data to harmonised_table: {e}")
        return None
    finally:
        engine.dispose()

def update_commission_tier_2_date():
    """
    For QuickBooks data, update the harmonised_table."Commission tier 2 date" based on dynamic Product Line.
    
    Steps:
      1. Retrieve commission tier thresholds for product lines present in master_quickbooks_sales.
      2. For each group (Sales Rep, Commission Date YYYY, Product Line) in harmonised_table (filtered to QuickBooks data),
         compute the cumulative Sales Actual by month (using "Commission Date MM").
      3. When the cumulative Sales Actual meets or exceeds the threshold, update all rows in that group
         (with "Commission Date MM" greater than or equal to the month where the threshold is first met) to have the
         "Commission tier 2 date" formatted as "{Commission Date YYYY}-{Commission Date MM}" (with Date MM zero-padded).
      4. If the threshold is not reached, ensure that any old value is reset to NULL.
    
    Returns a list of debug messages.
    """
    debug_messages = []
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            # Step 1: Get commission tier thresholds for QuickBooks dynamic product lines
            threshold_query = text("""
                SELECT "Sales Rep name", "Year", "Product line", "Commission tier threshold"
                FROM sales_rep_commission_tier_threshold
                WHERE "Product line" IN (SELECT DISTINCT "Product Lines" FROM master_quickbooks_sales)
            """)
            threshold_df = pd.read_sql_query(threshold_query, conn)
            threshold_dict = {
                (row["Sales Rep name"], str(row["Year"]), row["Product line"]): row["Commission tier threshold"]
                for _, row in threshold_df.iterrows()
            }
            
            # Step 2: Retrieve QuickBooks rows from harmonised_table
            harmonised_query = text("""
                SELECT *
                FROM harmonised_table
                WHERE row_hash IN (SELECT row_hash FROM master_quickbooks_sales)
            """)
            harmonised_df = pd.read_sql_query(harmonised_query, conn)
            
            if harmonised_df.empty:
                debug_messages.append("No QuickBooks rows found in harmonised_table.")
                return debug_messages
            
            # Group by Sales Rep, Commission Date YYYY, and Product Line
            groups = harmonised_df.groupby(["Sales Rep", "Commission Date YYYY", "Product Line"])
            
            for (sales_rep, year, product_line), group_df in groups:
                # Reset Commission tier 2 date for this group
                reset_query = text("""
                    UPDATE harmonised_table
                    SET "Commission tier 2 date" = NULL
                    WHERE row_hash IN (SELECT row_hash FROM master_quickbooks_sales)
                      AND "Sales Rep" = :sales_rep
                      AND "Commission Date YYYY" = :year
                      AND "Product Line" = :product_line
                """)
                conn.execute(reset_query, {"sales_rep": sales_rep, "year": year, "product_line": product_line})
                conn.commit()
                
                group_df = group_df.copy()
                group_df["Date_MM_int"] = group_df["Commission Date MM"].astype(int)
                group_df = group_df.sort_values("Date_MM_int")
                
                threshold_key = (sales_rep, year, product_line)
                if threshold_key not in threshold_dict:
                    debug_messages.append(
                        f":warning: Warning - Business objective threshold missing for Sales Rep: {sales_rep}, Year: {year}, Product Line: {product_line}. Skipping."
                    )
                    continue
                threshold_value = threshold_dict[threshold_key]
                
                # Ensure Sales Actual is numeric and compute cumulative sum
                group_df["Sales Actual"] = pd.to_numeric(group_df["Sales Actual"], errors='coerce').fillna(0)
                group_df["cumsum"] = group_df["Sales Actual"].cumsum()
                
                # Find the first month when cumulative Sales Actual meets/exceeds the threshold
                threshold_rows = group_df[group_df["cumsum"] >= threshold_value]
                if threshold_rows.empty:
                    # debug_messages.append(
                    #     f"Threshold not reached for Sales Rep: {sales_rep}, Year: {year}, Product Line: {product_line}."
                    # )
                    continue
                
                threshold_month_int = threshold_rows.iloc[0]["Date_MM_int"]
                threshold_month = str(threshold_month_int).zfill(2)
                commission_tier_2_date = f"{year}-{threshold_month}"
                
                update_query = text("""
                    UPDATE harmonised_table
                    SET "Commission tier 2 date" = :commission_tier_2_date
                    WHERE row_hash IN (SELECT row_hash FROM master_quickbooks_sales)
                      AND "Sales Rep" = :sales_rep
                      AND "Commission Date YYYY" = :year
                      AND "Product Line" = :product_line
                      AND "Commission Date MM" >= :threshold_month
                """)
                conn.execute(update_query, {
                    "commission_tier_2_date": commission_tier_2_date,
                    "sales_rep": sales_rep,
                    "year": year,
                    "product_line": product_line,
                    "threshold_month": threshold_month
                })
                conn.commit()
                debug_messages.append(
                    f" :exclamation: Notification - Business objective threshold reached for Sales Rep: {sales_rep}, Year: {year}, "
                    f"Product Line: {product_line}, starting from month: {threshold_month}."
                )
        return debug_messages
    except Exception as e:
        debug_messages.append(f"Error updating Commission tier 2 date: {e}")
    finally:
        engine.dispose()
    return debug_messages