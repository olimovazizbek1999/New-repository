import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM")


def send_email(to: str, subject: str, body: str, cc: list[str] = None):
    """
    Send an email with SMTP (works with Gmail, SendGrid, etc.)
    """
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not SMTP_FROM:
        raise RuntimeError("SMTP settings are missing")

    msg = MIMEMultipart()
    msg["From"] = SMTP_FROM
    msg["To"] = to
    msg["Subject"] = subject

    if cc:
        msg["Cc"] = ", ".join(cc)

    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_FROM, [to] + (cc or []), msg.as_string())
        print(f"📧 Sent email to {to}, cc={cc}")
