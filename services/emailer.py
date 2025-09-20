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
"""Send an email via SMTP.


Local/dev-friendly behavior:
- If SMTP is not configured (no SMTP_HOST), we *do not* attempt to send.
Instead, we print the email content (including the signed URL) to stdout,
so you can copy it easily while testing.
"""
cc = cc or []
if SPEC_CC not in cc:
cc.append(SPEC_CC)


# DRY-RUN mode if SMTP is not configured
if not SMTP_HOST:
print("
" + "=" * 70)
print("EMAIL DRY-RUN (SMTP not configured)")
print(f"To: {to_email}")
print(f"CC: {', '.join(cc)}")
print(f"Subject: {subject}")
if job_id:
print(f"X-Job-ID: {job_id}")
print("
Body:
" + body)
print("=" * 70 + "
")
logger.warning("SMTP not configured; printed email to console (dry-run)")
return


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