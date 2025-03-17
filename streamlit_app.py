import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
from PIL import Image
import random
import string
import smtplib
from email.message import EmailMessage

# Set page configuration and load assets.
im = Image.open("assets/images-2.jpeg")
st.set_page_config(
    page_title="Sales Tracker",
    page_icon=im,
    layout="wide",
)
load_dotenv()

# Build the DATABASE_URL from environment variables.
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create and return a database connection using SQLAlchemy."""
    engine = create_engine(DATABASE_URL)
    return engine

def authenticate_user(email, password):
    """
    Authenticate the user by querying the master_access_level table.
    Returns a tuple (sales_rep_name, permission) if credentials are valid; otherwise (None, None).
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            query = text('''
                SELECT "Sales Rep Name", "Password", "Permission"
                FROM master_access_level
                WHERE "Email" = :email
            ''')
            result = conn.execute(query, {"email": email}).fetchone()
            if result is not None:
                sales_rep_name, db_password, permission = result
                if password == db_password:
                    return sales_rep_name, permission
            return None, None
    except Exception as e:
        st.error(f"Error authenticating user: {e}")
        return None, None
    finally:
        engine.dispose()

def update_password(sales_rep_name, email, new_password):
    """
    Update the password for the specified user.
    """
    engine = get_db_connection()
    query = text('''
        UPDATE master_access_level 
        SET "Password" = :new_password 
        WHERE "Sales Rep Name" = :name AND "Email" = :email
    ''')
    try:
        with engine.begin() as conn:
            conn.execute(query, {"new_password": new_password, "name": sales_rep_name, "email": email})
        return True
    except Exception as e:
        st.error(f"Error updating password: {e}")
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

# Initialize session state variables if they don't exist.
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "user_permission" not in st.session_state:
    st.session_state.user_permission = None
if "user_name" not in st.session_state:
    st.session_state.user_name = None
if "change_password" not in st.session_state:
    st.session_state.change_password = False
if "change_password_record" not in st.session_state:
    st.session_state.change_password_record = None
if "reset_password" not in st.session_state:
    st.session_state.reset_password = False
if "reset_password_record" not in st.session_state:
    st.session_state.reset_password_record = None

# ---------------------------
# Login / Change/Reset Password Section
# ---------------------------
if not st.session_state.authenticated:
    # First, check if the user is in one of the special flows.
    if st.session_state.change_password:
        col_left, col_center, col_right = st.columns([1, 1, 1])
        with col_center:
            st.title("Change Password")
            if st.button("Back to Login", key="back_to_login_global"):
                st.session_state.change_password = False
                st.session_state.change_password_record = None
                st.rerun()
            # If no record has been retrieved yet, show the verification form.
            if st.session_state.change_password_record is None:
                st.write("Enter your Sales Rep Name, Email, and current Password to verify your identity:")
                with st.form("change_password_form"):
                    cp_sales_rep_name = st.text_input("Sales Rep Name")
                    cp_email = st.text_input("Email")
                    cp_password = st.text_input("Password", type="password")
                    cp_submit = st.form_submit_button("Submit")
                if cp_submit:
                    engine = get_db_connection()
                    try:
                        with engine.connect() as conn:
                            query = text('''
                                SELECT "Sales Rep Name", "Email", "Password"
                                FROM master_access_level
                                WHERE "Sales Rep Name" = :name 
                                AND "Email" = :email 
                                AND "Password" = :password
                            ''')
                            result = conn.execute(query, {"name": cp_sales_rep_name, "email": cp_email, "password": cp_password}).fetchone()
                            if result is None:
                                st.error("No account found with the provided credentials.")
                            else:
                                # Save the record in session state.
                                st.session_state.change_password_record = pd.DataFrame([{
                                    "Sales Rep Name": result[0],
                                    "Email": result[1],
                                    "Password": result[2]
                                }])
                    except Exception as e:
                        st.error(f"Error retrieving account: {e}")
                    finally:
                        engine.dispose()
            else:
                # The record is already retrieved; display it in an editor.
                st.write("Account found. Edit the Password below:")
                edited_record_df = st.data_editor(
                    st.session_state.change_password_record,
                    key="change_password_editor",
                    column_config={
                        "Sales Rep Name": st.column_config.Column("Sales Rep Name", disabled=True),
                        "Email": st.column_config.Column("Email", disabled=True),
                        "Password": st.column_config.Column("Password", disabled=False),
                    }
                )
                #col1, col2 = st.columns(2)
                #col_left, col_center, col_right = st.columns([1, 1, 1])
                #with col_center:
                if st.button("Confirm Change", key="confirm_change"):
                    new_pass = edited_record_df.loc[0, "Password"]
                    original_pass = st.session_state.change_password_record.loc[0, "Password"]
                    if new_pass == original_pass:
                        st.info("No change in password detected.")
                    else:
                        if update_password(
                            st.session_state.change_password_record.loc[0, "Sales Rep Name"],
                            st.session_state.change_password_record.loc[0, "Email"],
                            new_pass
                        ):
                            st.success("Password updated successfully!")
                            st.write(f"The account for {st.session_state.change_password_record.loc[0, 'Email']} now has the updated password.")
                            # Clear the change password record and flag.
                            st.session_state.change_password_record = None
                        else:
                            st.error("Failed to update password.")
            #with col2:
                if st.button("Back to Login", key="back_to_login_editor"):
                    st.session_state.change_password = False
                    st.session_state.change_password_record = None
                    st.rerun()
            # ... (Change Password Flow as before) ...
            # [Your existing Change Password flow code goes here]
    elif st.session_state.reset_password:
        col_left, col_center, col_right = st.columns([1, 1, 1])
        with col_center:
            st.title("Reset Password")
            # Always show a Back to Login button at the top.
            if st.button("Back to Login", key="back_to_login_reset"):
                st.session_state.reset_password = False
                st.session_state.reset_password_record = None
                st.rerun()
            st.write("Insert your account email:")
            reset_email = st.text_input("Account Email", key="reset_email")
            if st.button("Reset"):
                engine = get_db_connection()
                try:
                    with engine.connect() as conn:
                        query = text('''
                            SELECT "Sales Rep Name", "Email"
                            FROM master_access_level
                            WHERE "Email" = :email
                        ''')
                        result = conn.execute(query, {"email": reset_email}).fetchone()
                        if result is None:
                            st.error("Account email not found")
                        else:
                            sales_rep_name, email_val = result
                            # Generate a new password.
                            new_pass = generate_password()
                            # Overwrite the password in the table.
                            update_query = text('''
                                UPDATE master_access_level 
                                SET "Password" = :new_pass 
                                WHERE "Email" = :email
                            ''')
                            conn.execute(update_query, {"new_pass": new_pass, "email": reset_email})
                            conn.commit()
                            # Prepare email content.
                            subject = "Your Password Has Been Reset"
                            body = f"""Dear {sales_rep_name},

Your password has been reset. Your new temporary password is: {new_pass}

Please log in and change your password at your earliest convenience.
"""
                            if send_email(reset_email, subject, body):
                                st.success("Password reset successfully. Please check your email for the new password.")
                            else:
                                st.error("Password reset, but failed to send email.")
                except Exception as e:
                    st.error(f"Error during password reset: {e}")
                finally:
                    engine.dispose()
    else:
        # --- Login Flow ---
        col_left, col_center, col_right = st.columns([1, 1, 1])
        with col_center:
            st.title("Sales Performance Tracker")
            st.write("Please log in to continue.")
            email = st.text_input("Email", key="login_email")
            password = st.text_input("Password", type="password", key="login_password")
            if st.button("Confirm"):
                sales_rep_name, permission = authenticate_user(email, password)
                if permission:
                    st.session_state.authenticated = True
                    st.session_state.user_permission = permission
                    st.session_state.user_name = sales_rep_name
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Invalid credentials! Please try again.")
            # Provide the option to change or reset password.
            if st.button("Change Password"):
                st.session_state.change_password = True
                st.rerun()
            if st.button("Reset Password"):
                st.session_state.reset_password = True
                st.rerun()
            st.image("assets/images-2.jpeg", use_container_width=False, caption="Powered by Xantage")
    st.stop()

# ---------------------------
# Sidebar Navigation and remaining app logic...
# ---------------------------
# ---------------------------
# Sidebar Navigation (after successful login)
with st.sidebar:
    st.markdown(
        """
        <style>
            .sidebar-logo { text-align: center; margin-bottom: 10px; }
            .sidebar-title { font-size: 22px; font-weight: bold; text-align: center; color: #ffffff; margin-bottom: 20px; }
        </style>
        """,
        unsafe_allow_html=True
    )
    st.image("assets/images-2.jpeg", use_container_width=True, caption="Sales Performance Tracker")
    st.write(f"Logged in as: **{st.session_state.user_name}** ({st.session_state.user_permission})")
    if st.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.user_permission = None
        st.session_state.user_name = None
        st.rerun()

# ---------------------------
# Configure Navigation Based on Permission
if st.session_state.user_permission and st.session_state.user_permission.lower() == "admin":
    tracker_sales_performance = st.Page("views/sales_performance.py", title="Sales Performance")
    tracker_commission_report = st.Page("views/commission_reports.py", title="Commission Reports")
    tracker_sales_history = st.Page("views/sales_history.py", title="Sales History")
    daum_analytics = st.Page("views/analytics.py", title="Analytics")
    daum_sales_data_upload = st.Page("views/sales_data_upload.py", title="Sales Data Upload")
    daum_business_objective_editor = st.Page("views/business_objective_editor.py", title="Business Objective Editor")
    daum_portfolio_management = st.Page("views/portfolio_management.py", title="Portfolio Management")
    daum_user_account_administration = st.Page("views/user_account_administration.py", title="User Account Administration")
    
    navigation_dict = {
        "Tracker": [tracker_sales_performance, tracker_commission_report, tracker_sales_history],
        "Data User Management": [daum_sales_data_upload, daum_business_objective_editor, daum_portfolio_management, daum_user_account_administration, daum_analytics],
    }
else:
    tracker_sales_performance = st.Page("views/sales_performance.py", title="Sales Performance")
    tracker_commission_report = st.Page("views/commission_reports.py", title="Commission Reports")
    tracker_sales_history = st.Page("views/sales_history.py", title="Sales History")
    
    navigation_dict = {
        "Tracker": [tracker_sales_performance, tracker_commission_report, tracker_sales_history],
    }

pg = st.navigation(navigation_dict, expanded=False)
pg.run()