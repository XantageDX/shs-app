# import pandas as pd
# from sqlalchemy import create_engine
# from dotenv import load_dotenv
# import os

# # Step 1: Load environment variables
# load_dotenv()

# # Step 2: Define database connection URL
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# # Step 3: Create a connection to the database
# engine = create_engine(DATABASE_URL)

# # Step 4: Load the Excel file into a pandas DataFrame
# file_path = "Portfolio Management .xlsx"  # Replace with the correct path to your Excel file
# try:
#     df = pd.read_excel(file_path)
#     print("Excel file loaded successfully!")
# except Exception as e:
#     print(f"Error loading Excel file: {e}")
#     exit()

# # Step 5: Validate the DataFrame (Optional)
# print("Preview of the data to be uploaded:")
# print(df.head())

# # Step 6: Write the DataFrame to the PostgreSQL table
# try:
#     df.to_sql("service_to_product", con=engine, if_exists="replace", index=False)  # Use "append" to add data without overwriting
#     print("Data successfully loaded into the service_to_product table!")
# except Exception as e:
#     print(f"Error inserting data into the database: {e}")
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Step 1: Load environment variables
load_dotenv()

# Step 2: Define database connection parameters
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')  # Should be 127.0.0.1
DB_PORT = os.getenv('DB_PORT')  # Should be 5433
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Connecting to database with: {DATABASE_URL}")

# Step 3: Create the connection engine
engine = create_engine(DATABASE_URL)

# Step 4: Load the Excel file into a DataFrame
file_path = '/Users/renatomoretti/Downloads/Portfolio Management -2.xlsx'
try:
    df = pd.read_excel(file_path)
    print("Excel file loaded successfully!")
except Exception as e:
    print(f"Error loading Excel file: {e}")
    exit()

# Step 5: Preview the data
print("Preview of the data to be uploaded:")
print(df.head())

# Step 6: Write the DataFrame to the PostgreSQL table
try:
    df.to_sql("service_to_product", con=engine, if_exists="replace", index=False)
    print("Data successfully loaded into the service_to_product table!")
except Exception as e:
    print(f"Error inserting data into the database: {e}")
