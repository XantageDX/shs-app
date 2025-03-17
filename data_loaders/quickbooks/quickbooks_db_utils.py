import os
import hashlib
from sqlalchemy import create_engine, text
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
    (For QuickBooks these include Date, Product Lines, Service Lines, Customer and Company name.)
    """
    columns_to_hash = [
        "Date", "Product Lines", "Service Lines", "Customer", "Company name"
    ]
    row_data = ''.join([str(row[col]) for col in columns_to_hash if col in row]).encode('utf-8')
    return hashlib.sha256(row_data).hexdigest()

def save_dataframe_to_db(df: pd.DataFrame, table_name: str = "master_quickbooks_sales"):
    """
    Save data to the master_quickbooks_sales table by removing duplicates based on row_hash.
    Then, update the harmonised_table by mapping the new data and recalculating commission tier 2 dates.
    
    Returns a list of debug messages.
    """
    engine = get_db_connection()
    debug_messages = []

    # Generate row_hash for each row
    df["row_hash"] = df.apply(generate_row_hash, axis=1)
    
    try:
        with engine.connect() as conn:
            # Get the list of row_hashes already stored
            existing_hashes_query = text(f"SELECT row_hash FROM {table_name}")
            existing_hashes = pd.read_sql(existing_hashes_query, conn)['row_hash'].tolist()

            # Only keep new rows (by row_hash)
            new_rows = df[~df["row_hash"].isin(existing_hashes)]

            if not new_rows.empty:
                new_rows.to_sql(table_name, con=engine, if_exists="append", index=False)
                debug_messages.append(f"✅ New rows successfully added to '{table_name}'.")
            else:
                debug_messages.append(f"⚠️ No new rows to add to '{table_name}'.")

            # Update harmonised_table with the newly mapped QuickBooks data
            harmonised_messages = update_harmonised_table(table_name)
            debug_messages.extend(harmonised_messages)
            
            # Now update the Commission tier 2 date for QuickBooks data.
            if table_name == "master_quickbooks_sales":
                commission_tier_2_messages = update_commission_tier_2_date()
                debug_messages.extend(commission_tier_2_messages)

    except SQLAlchemyError as e:
        debug_messages.append(f"❌ Error saving data to '{table_name}': {e}")
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
      - "Date"          from QuickBooks is kept as "Date"
      - "Date MM"       from QuickBooks is kept as "Date MM"
      - "Date YYYY"     from QuickBooks is kept as "Date YYYY"
      - "Sales Rep Name" from QuickBooks is mapped to "Sales Rep"
      - "Product Lines" from QuickBooks becomes "Product Line"
      - "Amount line"   from QuickBooks becomes "Sales Actual"
      - "Margin"        from QuickBooks becomes "Rev Actual"
    
    Additional calculations for commission amounts and SHS Margin are performed.
    A literal column "Data Source" is added with the value 'master_quickbooks_sales'.
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
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
        "Date YYYY",
        "Product Line",
        SUM("Rev Actual") AS total_revenue_ytd
    FROM harmonised_table
    WHERE "Product Line" IN (SELECT DISTINCT "Product Lines" FROM master_quickbooks_sales) 
      AND "Date MM"::INTEGER BETWEEN 1 AND (SELECT MAX("Date MM"::INTEGER) FROM master_quickbooks_sales) 
    GROUP BY "Sales Rep", "Date YYYY", "Product Line"
),
commission_calculations AS (
    SELECT 
        mqs."Date",
        mqs."Date MM",
        mqs."Date YYYY",
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
        AND mqs."Date YYYY"::INTEGER = thr."Year"
    LEFT JOIN cumulative_revenue AS c
        ON mqs."Sales Rep Name" = c."Sales Rep"
        AND mqs."Date YYYY"::INTEGER = c."Date YYYY"::INTEGER
        AND mqs."Product Lines" = c."Product Line"  
),
tier_2_eligibility AS (
    SELECT 
        "Sales Rep Name",
        "Date YYYY",
        "Product Lines",
        MIN("Date") AS "Commission tier 2 date"
    FROM commission_calculations
    WHERE (total_revenue_ytd + "Margin") >= "Commission tier threshold"
    GROUP BY "Sales Rep Name", "Date YYYY", "Product Lines"
)
SELECT 
    c."Date" AS "Date",
    c."Date MM",
    c."Date YYYY",
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
    AND c."Date YYYY"::INTEGER = t2."Date YYYY"::INTEGER
    AND c."Product Lines" = t2."Product Lines";

            """)
            result = conn.execute(query)
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
      2. For each group (Sales Rep, Date YYYY, Product Line) in harmonised_table (filtered to QuickBooks data),
         compute the cumulative Sales Actual by month (using "Date MM").
      3. When the cumulative Sales Actual meets or exceeds the threshold, update all rows in that group
         (with "Date MM" greater than or equal to the month where the threshold is first met) to have the
         "Commission tier 2 date" formatted as "{Date YYYY}-{Date MM}" (with Date MM zero-padded).
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
            
            # Group by Sales Rep, Date YYYY, and Product Line
            groups = harmonised_df.groupby(["Sales Rep", "Date YYYY", "Product Line"])
            
            for (sales_rep, year, product_line), group_df in groups:
                # Reset Commission tier 2 date for this group
                reset_query = text("""
                    UPDATE harmonised_table
                    SET "Commission tier 2 date" = NULL
                    WHERE row_hash IN (SELECT row_hash FROM master_quickbooks_sales)
                      AND "Sales Rep" = :sales_rep
                      AND "Date YYYY" = :year
                      AND "Product Line" = :product_line
                """)
                conn.execute(reset_query, {"sales_rep": sales_rep, "year": year, "product_line": product_line})
                conn.commit()
                
                group_df = group_df.copy()
                group_df["Date_MM_int"] = group_df["Date MM"].astype(int)
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
                      AND "Date YYYY" = :year
                      AND "Product Line" = :product_line
                      AND "Date MM" >= :threshold_month
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

