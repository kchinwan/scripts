import pandas as pd
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine

# -----------------------
# DB Configuration
# -----------------------
DB_CONFIG = {
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 3306,
    'database': 'patching_db'
}

def get_engine():
    from sqlalchemy import create_engine
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

# -----------------------
# SMTP Configuration
# -----------------------
SMTP_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'your_email@gmail.com',
    'sender_password': 'your_password_or_app_password'
}

# -----------------------
# Get today's batches
# -----------------------
def get_batches_for_today():
    today = datetime.date.today()
    engine = get_engine()
    query = f"""
        SELECT * FROM patch_schedule
        WHERE patch_schedule_time = '{today}'
    """
    df = pd.read_sql(query, con=engine)
    return df

# -----------------------
# Create email HTML body
# -----------------------
def create_email_body(batch_id, servers_df):
    # Generate table rows
    table_rows = ""
    for _, row in servers_df.iterrows():
        table_rows += f"""
            <tr>
                <td>{row['hostname']}</td>
                <td>{row['ip_address']}</td>
                <td>{row['application_name']}</td>
                <td>{row['environment']}</td>
                <td>{row['patch_schedule_time']}</td>
            </tr>
        """

    # HTML body
    html = f"""
    <html>
        <body>
            <p>Dear Approver,</p>
            <p>Please review and approve the patching schedule for the following servers:</p>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Hostname</th>
                    <th>IP Address</th>
                    <th>Application</th>
                    <th>Environment</th>
                    <th>Scheduled Time</th>
                </tr>
                {table_rows}
            </table>
            <br>
            <p>
                <a href="http://your-server.com/approve?batch_id={batch_id}" style="padding:10px 20px; background-color:green; color:white; text-decoration:none;">✅ Approve</a>
                &nbsp;&nbsp;
                <a href="http://your-server.com/propose?batch_id={batch_id}" style="padding:10px 20px; background-color:orange; color:white; text-decoration:none;">⏱ Propose New Time</a>
            </p>
            <p>Regards,<br>Patch Automation Bot</p>
        </body>
    </html>
    """
    return html

# -----------------------
# Send Email
# -----------------------
def send_email(to_email, subject, html_body):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_CONFIG['sender_email']
    msg["To"] = to_email

    part = MIMEText(html_body, "html")
    msg.attach(part)

    with smtplib.SMTP(SMTP_CONFIG['smtp_server'], SMTP_CONFIG['smtp_port']) as server:
        server.starttls()
        server.login(SMTP_CONFIG['sender_email'], SMTP_CONFIG['sender_password'])
        server.sendmail(SMTP_CONFIG['sender_email'], to_email, msg.as_string())

# -----------------------
# Main
# -----------------------
def main():
    today_df = get_batches_for_today()
    if today_df.empty:
        print("No batches scheduled for today.")
        return

    grouped = today_df.groupby("batch_id")

    for batch_id, batch_df in grouped:
        approver_email = batch_df.iloc[0]['approver_email']
        print(f"📧 Sending email for batch {batch_id} to {approver_email}")

        html_body = create_email_body(batch_id, batch_df)
        subject = f"Approval Needed: Patching Batch {batch_id}"
        send_email(approver_email, subject, html_body)

    print("All emails sent successfully.")


if __name__ == "__main__":
    main()
