import pandas as pd

# You might also want to define EXPECTED_COLUMNS here or in another config file.# Serving the function validate_file_format()
EXPECTED_COLUMNS = {
    "Cygnus": [
        "Sales Rep",
        "Cust. ID",
        "Cust- Name",
        "Name",
        "Address",
        "City",
        "State",
        "Invoice",
        "SKU",
        "Inv Date",
        "Due Date",
        "ClosedDate",
        "Days Past",
        "Rep %",
        "Invoice Total",
        "Total Rep Due"
    ],
    "Logiquip": [
        "Agency",
        "Rep",
        "Doc Num",
        #"Unnamed: 3",  # This is the empty header column that Excel created.
        "Customer",
        "PO Number",
        "Ship To Zip",
        "Date Paid",
        "Contract",
        "Item Class",
        "Comm Rate",
        "Doc Amt",
        "Comm Amt"
    ],
    "QuickBooks": [
        "Date",
        "Service Lines",
        "Customer",
        "Transaction type",
        #"Company name",
        "Amount line",
        "Purchase price",
        "Quantity",
        "Num",
        "Line order",
        #"Description",
        "Purchase description",
        "Sales Rep Name",
        "Sales Rep Territory",
        "Product/Service"
    ],
    "Summit Medical": [ # Don't count index column!
        "Client Name",
        "Invoice #",
        "Item ID",
        "Net Sales Amount",
        "Comm Rate",
        "Comm $",
        "Sales Rep Code",
        "State",
        "ZIP Code",
        "Date",
        "Date MM",
        "Date YYYY",
        "Sales Rep Name"
    ],
    "InspeKtor": [ # Don't count index column!
        #"Sales Rep",
        #"Name",
        "Company",
        "Date",
        "Document Number",
        "Customer:Project",
        "Item: Name",
        "Description",
        "Quantity",
        "Total",
        "Commission %",
        "Formula",
        "Ship To"
    ],
    "Sunoptic": [
        "Invoice ID",
        "Invoice Date",
        "Customer ID",
        "Bill Name",
        "Sales Order ID",
        "Item ID",
        "Item Name",
        "Prod Fam",
        "Unit Price",
        "Ship Qty",
        "Customer Type",
        "Ship To Name",
        "Address Ship to",
        "Ship To City",
        "Ship To State",
        "Sales Rep Name",
        "Line Amount",
        "Commission %",
        "Commission $"
    ]
}

# def validate_file_format(df: pd.DataFrame, file_type: str):
#     """
#     Checks if the DataFrame contains all expected columns for the given file type.
#     Returns (is_valid, missing_columns) where is_valid is True if all expected columns are present.
#     """
#     expected = set(EXPECTED_COLUMNS.get(file_type, []))
#     actual = set(df.columns)
#     missing = list(expected - actual)
#     return (len(missing) == 0, missing)
def validate_file_format(df: pd.DataFrame, file_type: str):
    """
    Checks if the DataFrame (with normalized column names) contains all expected columns for the given file type.
    Returns (is_valid, missing_columns) where is_valid is True if all expected columns are present.
    """
    expected = {col.strip() for col in EXPECTED_COLUMNS.get(file_type, [])}
    actual = {col.strip() for col in df.columns}
    missing = list(expected - actual)
    return (len(missing) == 0, missing)

