import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import List, Optional


logger = logging.getLogger(__name__)


SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER or "no-reply@example.com")


# Always CC this address per spec
SPEC_CC = "olimovazizbek1999@gmail.com"




def send_email(to_email: str, subject: str, body: str, cc: Optional[List[str]] = None, job_id: Optional[str] = None):
if not SMTP_HOST:
logger.warning("SMTP not configured; skipping email send")
return
cc = cc or []
if SPEC_CC not in cc:
cc.append(SPEC_CC)


msg = EmailMessage()
msg["From"] = SMTP_FROM
msg["To"] = to_email
if cc:
msg["Cc"] = ", ".join(cc)
msg["Subject"] = subject
if job_id:
msg["X-Job-ID"] = job_id
msg.set_content(body)


context = ssl.create_default_context()
with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
server.starttls(context=context)
if SMTP_USER and SMTP_PASS:
server.login(SMTP_USER, SMTP_PASS)
server.send_message(msg)
logger.info(f"Email sent to {to_email} (cc {cc})")