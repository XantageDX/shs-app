import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def generate_row_hash(row: pd.Series) -> str:
    """Generate a hash including key columns and 'Enriched'."""
    columns_to_hash = ["Client Name", "Invoice #", "Item ID", "Sales Rep Name", "Date", "ZIP Code"]
    row_data = ''.join([str(row[col]) for col in columns_to_hash if col in row]).encode('utf-8')
    return hashlib.sha256(row_data).hexdigest()

def map_summit_medical_to_harmonised():
    """
    Map Cygnus data from the 'master_summit_medical_sales' table to the harmonised_table structure and transfer the hash.
    
    New logic:
      - Join master_summit_medical_sales with commission rates from sales_rep_commission_tier.
      - Calculate:
            "Comm Amount tier 1" = "Comm $" * "Commission tier 1 rate"
            "Comm tier 2 diff amount" = ("Comm $" * "Commission tier 2 rate") - ("Comm $" * "Commission tier 1 rate")
      - Select and rename columns as follows:
            "Date"       AS Date,
            "Date YYYY"  AS "Date YYYY",
            "Date MM"    AS "Date MM",
            "Sales Rep Name"   AS "Sales Rep",
            "Net Sales Amount"    AS "Sales Actual",
            "Comm $"    AS "Rev Actual",
            'Summit Medical'           AS "Product Line",
            'master_summit_medical_sales' AS "Data Source",
            row_hash,
            "Comm Amount tier 1",
            "Comm tier 2 diff amount"
            
    (Temporarily omitting SHS Margin and Commission tier 2 date.)
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
commission_calculations AS (
    SELECT 
        mcs."Date",
        mcs."Date MM",
        mcs."Date YYYY",
        mcs."Sales Rep Name",
        mcs."Net Sales Amount",
        mcs."Comm $",
        CAST((mcs."Comm $" * crt."Commission tier 1 rate") AS NUMERIC(15,2)) AS "Comm Amount tier 1",
        CAST((mcs."Comm $" * crt."Commission tier 2 rate" - mcs."Comm $" * crt."Commission tier 1 rate") AS NUMERIC(15,2)) AS "Comm tier 2 diff amount",
        mcs.row_hash
    FROM master_summit_medical_sales AS mcs
    LEFT JOIN commission_rates AS crt
        ON mcs."Sales Rep Name" = crt."Sales Rep Name"
)
SELECT 
    "Date" AS "Date",
    "Date MM" AS "Date MM",
    "Date YYYY" AS "Date YYYY",
    "Sales Rep Name" AS "Sales Rep",
    "Net Sales Amount" AS "Sales Actual",
    "Comm $" AS "Rev Actual",
    'Summit Medical' AS "Product Line",
    'master_summit_medical_sales' AS "Data Source",
    row_hash,
    "Comm Amount tier 1",
    "Comm tier 2 diff amount"
FROM commission_calculations;
            """)
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except SQLAlchemyError as e:
        print(f"❌ Error fetching and mapping Sumit Medical data: {e}")
        return None
    finally:
        engine.dispose()

def save_dataframe_to_db(df: pd.DataFrame, table_name: str = "master_summit_medical_sales"):
    """
    Save data to the 'master_summit_medical_sales' table by removing entries based on 'Date MM' and 'Date YYYY'.
    Return debug messages as a list.
    """
    table_name = table_name.lower()
    engine = get_db_connection()
    debug_messages = []
    # Generate row_hash for each row
    df["row_hash"] = df.apply(generate_row_hash, axis=1)
    
    try:
        with engine.connect() as conn:
            # Identify the "Date MM" and "Date YYYY" values from the confirmed dataframe
            closed_date_values = df[['Date MM', 'Date YYYY']].drop_duplicates().values.tolist()

            # Convert the closed_date_values into a filterable SQL condition
            condition = " OR ".join(
                [f'("Date MM" = \'{mm}\' AND "Date YYYY" = \'{yyyy}\')' 
                 for mm, yyyy in closed_date_values]
            )

            # Delete existing records matching those dates
            delete_query = text(f"DELETE FROM {table_name} WHERE {condition}")
            conn.execute(delete_query)
            conn.commit()
            print(f"✅ Deleted records from '{table_name}' matching specified Date values.")
            debug_messages.append(f"✅ Deleted records from '{table_name}' matching specified Date values.")

            # Append the confirmed dataframe to the table
            df.to_sql(table_name, con=engine, if_exists="append", index=False)
            print(f"✅ New data successfully added to '{table_name}'.")
            debug_messages.append(f"✅ New data successfully added to '{table_name}'.")

        # If the table is 'master_summit_medical_sales', update the harmonised table
        if table_name == "master_summit_medical_sales":
            harmonised_messages = update_harmonised_table("master_summit_medical_sales")
            debug_messages.extend(harmonised_messages)
            
            # Now, after updating the harmonised table, update Commission tier 2 date.
            commission_tier_2_messages = update_commission_tier_2_date()
            debug_messages.extend(commission_tier_2_messages)

    except SQLAlchemyError as e:
        print(f"❌ Error saving data to '{table_name}': {e}")
        debug_messages.append(f"❌ Error saving data to '{table_name}': {e}")
    finally:
        engine.dispose()

    return debug_messages

def update_harmonised_table(table_name: str):
    """
    Harmonise the specific table ('master_summit_medical_sales') and update the harmonised_table.
    Return debug messages as a list.
    """
    debug_messages = []
    if table_name == "master_summit_medical_sales":
        harmonised_data = map_summit_medical_to_harmonised()
        if harmonised_data is not None:
            engine = get_db_connection()
            try:
                with engine.connect() as conn:
                    # Identify the Product Line
                    product_line = "Summit Medical"
                    data_source = "master_summit_medical_sales"

                    # Delete existing rows for the same product line in harmonised_table
                    delete_query = text("""DELETE FROM harmonised_table WHERE "Product Line" = :product_line AND "Data Source" = :data_source""")
                    conn.execute(delete_query, {"product_line": product_line, "data_source": data_source})
                    conn.commit()
                    print(f"✅ Deleted existing rows in 'harmonised_table' for Product Line: {product_line} with Data Source {data_source}.")
                    debug_messages.append(f"✅ Deleted existing rows in 'harmonised_table' for Product Line: {product_line} with Data Source {data_source}.")

                    # Append the newly harmonised data
                    harmonised_data.to_sql("harmonised_table", con=engine, if_exists="append", index=False)
                    print(f"✅ Harmonised table updated with new data for '{table_name}'.")
                    debug_messages.append(f"✅ Harmonised table updated with new data for '{table_name}'.")
                    
            except SQLAlchemyError as e:
                print(f"❌ Error updating harmonised_table: {e}")
                debug_messages.append(f"❌ Error updating harmonised_table: {e}")
            finally:
                engine.dispose()
        else:
            print(f"⚠️ No harmonised data available for '{table_name}'.")
            debug_messages.append(f"⚠️ No harmonised data available for '{table_name}'.")
            
    return debug_messages

def update_commission_tier_2_date():
    """
    For Product Line 'Summit Medical', update the harmonised_table."Commission tier 2 date" as follows:
    
      1. For each distinct Sales Rep and year ("Date YYYY"), retrieve all rows (ordered by "Date MM" ascending).
      2. Look up the commission tier threshold from sales_rep_commission_tier_threshold.
      3. Compute the cumulative sum of "Sales Actual" (month by month).
      4. When the cumulative sum reaches or exceeds the tier threshold for the first time (say in month_n),
         update all rows in that group (i.e. for that Sales Rep and year) having "Date MM" >= month_n with:
             "{Date YYYY}-{Date MM}"
         where Date MM is taken from the first month where the threshold was met.
         
      Additionally, if the threshold is not reached, ensure that any old value is overwritten with NULL.
    
    Returns a list of debug messages.
    """
    debug_messages = []
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            # Step 1: Get commission tier thresholds for Product Line 'Summit Medical'
            threshold_query = text("""
                SELECT "Sales Rep name", "Year", "Commission tier threshold"
                FROM sales_rep_commission_tier_threshold
                WHERE "Product line" = 'Summit Medical'
            """)
            threshold_df = pd.read_sql_query(threshold_query, conn)
            # Create a dictionary keyed by (Sales Rep, Year) with the threshold value.
            threshold_dict = {
                (row["Sales Rep name"], str(row["Year"])): row["Commission tier threshold"]
                for _, row in threshold_df.iterrows()
            }
            
            # Step 2: Retrieve all rows from harmonised_table for Product Line 'Summit Medical'
            harmonised_query = text("""
                SELECT *
                FROM harmonised_table
                WHERE "Product Line" = 'Summit Medical'
            """)
            harmonised_df = pd.read_sql_query(harmonised_query, conn)
            
            if harmonised_df.empty:
                debug_messages.append("No Summit Medical rows found in harmonised_table.")
                return debug_messages
            
            # Process by grouping rows by Sales Rep and Year ("Date YYYY")
            groups = harmonised_df.groupby(["Sales Rep", "Date YYYY"])
            
            for (sales_rep, year), group_df in groups:
                # First, reset Commission tier 2 date to NULL for this Sales Rep and Year.
                reset_query = text("""
                    UPDATE harmonised_table
                    SET "Commission tier 2 date" = NULL
                    WHERE "Product Line" = 'Summit Medical'
                      AND "Sales Rep" = :sales_rep
                      AND "Date YYYY" = :year
                """)
                conn.execute(reset_query, {"sales_rep": sales_rep, "year": year})
                conn.commit()
                
                group_df = group_df.copy()
                # Convert "Date MM" to integer for proper sorting
                group_df["Date_MM_int"] = group_df["Date MM"].astype(int)
                group_df = group_df.sort_values("Date_MM_int")
                
                threshold_key = (sales_rep, year)
                if threshold_key not in threshold_dict:
                    debug_messages.append(f":warning: Warning - Business objective threshold missing for Sales Rep: {sales_rep}, Year: {year}. Skipping.")
                    continue
                threshold_value = threshold_dict[threshold_key]
                
                # Ensure Sales Actual is numeric and compute cumulative sum
                group_df["Sales Actual"] = pd.to_numeric(group_df["Sales Actual"], errors='coerce').fillna(0)
                group_df["cumsum"] = group_df["Sales Actual"].cumsum()
                
                # Find the first row where cumulative sales meet or exceed the threshold
                threshold_rows = group_df[group_df["cumsum"] >= threshold_value]
                if threshold_rows.empty:
                    #debug_messages.append(f"Threshold not reached for Sales Rep: {sales_rep}, Year: {year}.")
                    continue
                
                # The month where the threshold is first reached
                threshold_month_int = threshold_rows.iloc[0]["Date_MM_int"]
                threshold_month = str(threshold_month_int).zfill(2)
                commission_tier_2_date = f"{year}-{threshold_month}"
                
                # Update all rows in harmonised_table for this Sales Rep and Year with "Date MM" >= threshold_month
                update_query = text("""
                    UPDATE harmonised_table
                    SET "Commission tier 2 date" = :commission_tier_2_date
                    WHERE "Product Line" = 'Summit Medical'
                      AND "Sales Rep" = :sales_rep
                      AND "Date YYYY" = :year
                      AND "Date MM" >= :threshold_month
                """)
                conn.execute(update_query, {
                    "commission_tier_2_date": commission_tier_2_date,
                    "sales_rep": sales_rep,
                    "year": year,
                    "threshold_month": threshold_month
                })
                conn.commit()
                debug_messages.append(
                    f" :exclamation: Notification - Business objective threshold reached for Sales Rep: {sales_rep}, Year: {year}, starting from month: {threshold_month}."
                )
    except Exception as e:
        debug_messages.append(f"Error updating Commission tier 2 date: {e}")
    finally:
        engine.dispose()
    return debug_messages
