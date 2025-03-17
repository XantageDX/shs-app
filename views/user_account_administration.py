import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import random
import string
import smtplib
from email.message import EmailMessage

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def insert_new_account(name, email, password, permission):
    """
    Insert a new user account into the master_access_level table.
    Expects columns: "Sales Rep Name", "Email", "Password", "Permission".
    """
    query = text('''
        INSERT INTO master_access_level ("Sales Rep Name", "Email", "Password", "Permission")
        VALUES (:name, :email, :password, :permission)
    ''')
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            conn.execute(query, {"name": name, "email": email, "password": password, "permission": permission})
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error inserting new account: {e}")
        return False
    finally:
        engine.dispose()

def generate_password():
    """
    Generate a random password in the format ABC123def:
      - Three uppercase letters,
      - Three digits,
      - Three lowercase letters.
    """
    part1 = ''.join(random.choices(string.ascii_uppercase, k=3))
    part2 = ''.join(random.choices(string.digits, k=3))
    part3 = ''.join(random.choices(string.ascii_lowercase, k=3))
    return part1 + part2 + part3

def send_email(recipient, subject, body):
    """
    Send an email using SMTP.
    Ensure that SMTP_HOST, SMTP_PORT, SMTP_USER, and SMTP_PASSWORD are set in your environment.
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    #msg["From"] = smtp_user
    msg["From"] = '"Xantage No-Reply" <no-reply@xantage.co>'
    msg["To"] = recipient
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True
    except Exception as e:
        st.error(f"Error sending email: {e}")
        return False

def delete_account(email):
    """
    Delete an account from master_access_level using the given email.
    Logs the number of affected rows for debugging.
    """
    query = text('DELETE FROM master_access_level WHERE "Email" = :email')
    engine = get_db_connection()
    try:
        with engine.begin() as conn:
            result = conn.execute(query, {"email": email})
            #st.write(f"DEBUG: Deleted row count for {email}: {result.rowcount}")
        return True
    except Exception as e:
        st.error(f"Error deleting account ({email}): {e}")
        return False
    finally:
        engine.dispose()

def fetch_accounts():
    """Fetch accounts with columns needed for deletion."""
    query = text('SELECT id, "Sales Rep Name", "Email", "Permission" FROM master_access_level')
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Error fetching accounts: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def email_exists(new_email, user_id):
    """
    Check if the new email already exists in another account.
    Returns True if the email exists (for a different account), False otherwise.
    """
    query = text('''
        SELECT COUNT(*) FROM master_access_level
        WHERE "Email" = :new_email AND id != :user_id
    ''')
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            count = conn.execute(query, {"new_email": new_email, "user_id": user_id}).scalar()
            return count > 0
    except Exception as e:
        st.error(f"Error checking for duplicate email: {e}")
        return True
    finally:
        engine.dispose()

def update_account(user_id, new_name, new_email, new_permission):
    """
    Update an account using its unique id.
    Checks for duplicate emails before updating.
    """
    if email_exists(new_email, user_id):
        st.error(f"Error: The email {new_email} is already in use by another account.")
        return False

    query = text('''
        UPDATE master_access_level
        SET "Sales Rep Name" = :new_name,
            "Email" = :new_email,
            "Permission" = :new_permission
        WHERE id = :user_id
    ''')
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            conn.execute(query, {
                "new_name": new_name,
                "new_email": new_email,
                "new_permission": new_permission,
                "user_id": user_id
            })
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error updating account for id {user_id}: {e}")
        return False
    finally:
        engine.dispose()

def delete_account_by_id(user_id):
    """
    Delete an account from master_access_level using the unique id.
    """
    query = text('DELETE FROM master_access_level WHERE id = :user_id')
    engine = get_db_connection()
    try:
        with engine.begin() as conn:
            result = conn.execute(query, {"user_id": user_id})
            st.write(f"DEBUG: Deleted row count for id {user_id}: {result.rowcount}")
        return True
    except Exception as e:
        st.error(f"Error deleting account for id {user_id}: {e}")
        return False
    finally:
        engine.dispose()

# ---------------------------------
# Combined Edit/Delete User Accounts Section
# ---------------------------------
st.title("User Account Administration")
st.subheader("Edit/Delete User Accounts")
st.write(
    "Modify the fields below. "
    "Check the 'Delete' box to mark a row for deletion. "
    "Other modifications will update the account. "
    "For security reasons, passwords are not displayed and remain unchanged. "
    "The unique id is used internally and is hidden from view."
)

# Fetch current accounts (with 'id' as unique identifier)
accounts_df = fetch_accounts()  # Returns columns: id, Sales Rep Name, Email, Permission
if not accounts_df.empty:
    # Set the unique identifier as the DataFrame index so it's hidden.
    original_df = accounts_df.set_index("id")
    
    # Create a new DataFrame for editing that includes a 'Delete' column, defaulting to False.
    df_for_edit = original_df.copy()
    df_for_edit["Delete"] = False

    # Configure the Permission column to use a dropdown menu.
    column_config = {
        "Permission": st.column_config.SelectboxColumn("Permission", options=["admin", "user"])
    }
    
    # Display a single editable DataFrame.
    # hide_index=True hides the index (the id) from the admin's view.
    edited_df = st.data_editor(
        df_for_edit,
        hide_index=True,
        key="edit_editor",
        column_config=column_config
    )
    
    if st.button("Confirm Changes"):
        # Process updates and deletions
        for user_id in edited_df.index:
            row = edited_df.loc[user_id]
            if row["Delete"]:
                # Delete account if marked for deletion.
                if delete_account_by_id(user_id):
                    st.success(f"Deleted account: {original_df.loc[user_id, 'Email']}")
                else:
                    st.error(f"Failed to delete account: {original_df.loc[user_id, 'Email']}")
            else:
                # Otherwise, check if any editable fields have changed.
                original_row = original_df.loc[user_id]
                if (original_row["Sales Rep Name"] != row["Sales Rep Name"] or
                    original_row["Email"] != row["Email"] or
                    original_row["Permission"] != row["Permission"]):
                    
                    if update_account(
                        user_id=user_id,
                        new_name=row["Sales Rep Name"],
                        new_email=row["Email"],
                        new_permission=row["Permission"]
                    ):
                        st.success(
                            f"The account {original_row['Sales Rep Name']}, email {original_row['Email']}, permission {original_row['Permission']} "
                            f"has been changed as follows: {row['Sales Rep Name']}, email {row['Email']}, permission {row['Permission']}"
                        )
                    else:
                        st.error(f"Failed to update account for id {user_id}")
        # Instead of immediately rerunning, show a button to refresh the data.
        st.info("Changes processed. Click 'Refresh Data' to update the view.")

    if st.button("Refresh Data"):
        st.rerun()

    st.markdown("---")
    # ---------------------------------
    # Create New User Accounts Section (unchanged)
    # ---------------------------------
    st.subheader("Create new User Accounts")
    st.write("Create a new user account by filling the fields. An automatic email with a temporary password will be sent.")
    
col_1, col_2, col_3 = st.columns([1, 1, 1])
with col_1:
    with st.form("new_account_form"):
        new_name = st.text_input("Sales Rep Name")
        new_email = st.text_input("Email")
        new_permission = st.selectbox("Permission", options=["admin", "user"], help="Select a permission level.")
        submitted = st.form_submit_button("Create New Account")
        
        if submitted:
            if new_name and new_email:
                new_password = generate_password()
                success = insert_new_account(new_name, new_email, new_password, new_permission)
                if success:
                    st.success(f"New account created successfully! Email sent to {new_email}")
                    subject = "Your new SHS Sales Performance Tracker account"
                    body = f"""Dear {new_name},

An account for your email has been created on the SHS Sales Performance Tracker app.

Your temporary password is {new_password}

Please, go to https://shs-app.xantage.co and click on the button "Change Password" and follow the procedure to choose your personal password.
"""
                    if send_email(new_email, subject, body):
                        st.info("An email with your account details has been sent.")
                    else:
                        st.error("Account created, but failed to send email.")
                else:
                    st.error("Failed to create new account.")
            else:
                st.error("Please fill in all required fields.")