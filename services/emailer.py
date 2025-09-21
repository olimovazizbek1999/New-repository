import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM")

def send_email(to_email: str, subject: str, body: str, job_id: str = ""):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and SMTP_FROM):
        logger.warning("SMTP not configured; skipping email")
        return

    msg = MIMEMultipart()
    msg["From"] = SMTP_FROM
    msg["To"] = to_email
    msg["Cc"] = "olimovazizbek1999@gmail.com"
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    recipients = [to_email, "olimovazizbek1999@gmail.com"]
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())
        logger.info(f"Email sent to {to_email} for job {job_id}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
