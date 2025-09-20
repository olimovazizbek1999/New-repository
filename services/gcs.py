import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Path to service account JSON key (stored outside git)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not GOOGLE_APPLICATION_CREDENTIALS or not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
    raise RuntimeError(
        "Google Cloud credentials file not found. "
        "Make sure GOOGLE_APPLICATION_CREDENTIALS is set in .env "
        "and points to a valid JSON key file."
    )

# Initialize storage client
storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)


def upload_fileobj(bucket_name: str, blob_name: str, file_obj):
    """Upload file object to Google Cloud Storage."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(file_obj)
    return f"gs://{bucket_name}/{blob_name}"


def download_file(bucket_name: str, blob_name: str, destination: str):
    """Download file from GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination)
    return destination


def list_files(bucket_name: str, prefix: str = ""):
    """List files in a GCS bucket under a given prefix."""
    bucket = storage_client.bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix)]
