import pandas as pd
from sqlalchemy import create_engine
from jinja2 import Environment, FileSystemLoader
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --------------------
# CONFIG
# --------------------
EMAIL_CONFIG = {
    "sender_email": "your-email@example.com",
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "smtp_user": "your-email@example.com",
    "smtp_pass": "your-email-password"
}

DB_CONFIG = {
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 3306,
    'database': 'patching_db'
}

APPROVAL_BASE_URL = "http://yourserver.com/patch"  # used in approve/propose links


# --------------------
# GET ENGINE
# --------------------
def get_engine():
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)


# --------------------
# FETCH UNSENT BATCHES
# --------------------
def get_unsent_batches():
    engine = get_engine()
    query = """
    SELECT * FROM patch_schedule
    WHERE approval_status = 'Pending'
    ORDER BY patch_date;
    """
    return pd.read_sql(query, engine)


# --------------------
# GENERATE EMAIL HTML
# --------------------
def render_email(batch_df, approver_email):
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("approval_email.html")

    batch_id = batch_df['batch_id'].iloc[0]
    patch_date = batch_df['patch_date'].iloc[0]
    app_name = batch_df['application_name'].iloc[0]

    approve_link = f"{APPROVAL_BASE_URL}/approve?batch_id={batch_id}"
    propose_link = f"{APPROVAL_BASE_URL}/propose?batch_id={batch_id}"

    html = template.render(
        approver_email=approver_email,
        batch_id=batch_id,
        patch_date=patch_date,
        servers=batch_df.to_dict(orient='records'),
        approve_link=approve_link,
        propose_link=propose_link
    )

    return html


# --------------------
# SEND EMAIL
# --------------------
def send_email(to_email, subject, html_content):
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = EMAIL_CONFIG["sender_email"]
    msg['To'] = to_email

    msg.attach(MIMEText(html_content, "html"))

    with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
        server.starttls()
        server.login(EMAIL_CONFIG["smtp_user"], EMAIL_CONFIG["smtp_pass"])
        server.sendmail(EMAIL_CONFIG["sender_email"], to_email, msg.as_string())

    print(f"📧 Email sent to {to_email}")


# --------------------
# MAIN DRIVER
# --------------------
def main():
    df = get_unsent_batches()

    if df.empty:
        print("✅ No pending batches to email.")
        return

    for batch_id, group in df.groupby('batch_id'):
        approver_email = "approver@example.com"  # Replace or fetch dynamically
        html = render_email(group, approver_email)
        subject = f"[PATCH APPROVAL] Batch {batch_id} on {group['patch_date'].iloc[0]}"
        send_email(approver_email, subject, html)

if __name__ == "__main__":
    main()
