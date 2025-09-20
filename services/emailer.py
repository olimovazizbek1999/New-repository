import smtplib
from email.message import EmailMessage
import os

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM")

def send_email(to, subject, body, cc=None):
    msg = EmailMessage()
    msg['From'] = SMTP_FROM
    msg['To'] = to
    msg['Subject'] = subject
    if cc:
        msg['Cc'] = cc
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
