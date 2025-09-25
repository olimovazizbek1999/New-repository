import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tenacity import retry, stop_after_attempt, wait_exponential

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def send_email(to: str, subject: str, body: str, cc: list[str] = None):
    """
    Send an email with SMTP (works with Gmail, Outlook, etc.).
    Retries up to 3 times on failure.
    """
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not SMTP_FROM:
        raise RuntimeError("SMTP settings are missing")

    msg = MIMEMultipart()
    msg["From"] = SMTP_FROM
    msg["To"] = to
    msg["Subject"] = subject

    recipients = [to]
    if cc:
        msg["Cc"] = ", ".join(cc)
        recipients.extend(cc)

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())
            print(f"📧 Sent email to {to}, cc={cc}")
    except Exception as e:
        print(f"⚠️ Email send failed: {e}")
        raise
