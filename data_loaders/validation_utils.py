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
        #"ClosedDate",
        "Days Past",
        "Rep %",
        "Invoice Total",
        "Total Rep Due"
    ],

    "Logiquip": [
        "Agency",
        "Rep",
        "Doc Num",
        "Customer",
        "PO Number",
        "Ship To Zip",
        #"Revenue Recognition Date", # Changed from "Date Paid"
        "Contract",
        "Item Class",
        "Comm Rate",
        "Doc Amt",
        "Comm Amt"
    ],

    "QuickBooks": [
        #"Revenue Recognition Date",  # We still expect Date in the input file
        "Service Lines",
        "Customer",
        "Transaction type",
        "Amount line",
        "Purchase price",
        "Quantity",
        "Num",
        "Line order",
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
        "Revenue Recognition Date",  # Keep original name for input validation since raw PDF files still have "Date"
        "Revenue Recognition Date MM",  # Keep original name for input validation since raw PDF files still have "Date MM" 
        "Revenue Recognition Date YYYY",  # Keep original name for input validation since raw PDF files still have "Date YYYY"
        "Sales Rep Name"
    ],
    "Summit Medical Excel": [
        "Row Labels",
        "St",
        "ZIP Code",
        "Invoice #",
        "Item",
        "CommRate",
        "Sum of Net Sales Amount",
        "Sum of Comm $"
    ],

    "InspeKtor": [ # Don't count index column!
            "Company",
            #"Date",  # We still expect "Date" in the input file
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
        #"Invoice Date",  # We still expect Invoice Date in the input file
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
    ],

    "Ternio": [
            "Client Name",
            "Commission Date",
            "Commission Date YYYY",
            "Commission Date MM",
            "Revenue Recognition Date",
            "Revenue Recognition Date YYYY",
            "Revenue Recognition Date MM",
            "Invoice Date",
            "Memo/Description",
            "Sales Rep Name",
            "Product Line",
            "Num",
            "Invoiced",
            "Paid",
            "Comm Rate",
            "Comm Amount"
        ],

    "Novo": [
        "Salesperson Number",
        "AR Division Number",
        "Customer Number",
        "Bill To Name",
        "Ship To Name",
        "Invoice Number",
        #"Invoice Date",  # Keep this as "Invoice Date" in the validation since that's what's in the raw file
        "Customer PO Number",
        "Item Code",
        "Alias Item Number",
        "Item Code Description",
        "Quantity Ordered",
        "Qty Shipped",
        "Quantity Backordered",
        "Unit Price",
        "Extension",
        "Comment",
        "Order Date",
        "Sales Order Number",
        "Ship To State",
        "Ship To Zip Code",
        "UD F LOTBUS",
        "Commission Percentage",
        "Commission Amount"
    ],

    "Chemence": [
        "Source",
        "Sales Group",
        "Source ID",
        "Account Number",
        "Account Name",
        "Street",
        "City",
        "State",
        "Zip",
        "Description",
        "Part #",
        #"Invoice Date",  # Keep using Invoice Date in EXPECTED_COLUMNS since that's what's in the raw file
        "Qty Shipped",
        "UOM",
        "Sales Price",
        "Sales Total",
        "Commission",
        "Unit Price",
        "Agreement"
    ]
}

def validate_file_format(df: pd.DataFrame, file_type: str):
    """
    Checks if the DataFrame (with normalized column names) contains all expected columns for the given file type.
    Returns (is_valid, missing_columns) where is_valid is True if all expected columns are present.
    """
    # Convert expected column names to strings and strip whitespace
    expected = {str(col).strip() for col in EXPECTED_COLUMNS.get(file_type, [])}
    
    # Convert actual column names to strings and strip whitespace
    actual = {str(col).strip() for col in df.columns}
    
    # Find missing columns
    missing = list(expected - actual)
    
    return (len(missing) == 0, missing)