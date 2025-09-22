import os
import datetime
import pandas as pd
from google.cloud import storage
from google.auth import default, impersonated_credentials

# Read env vars
SERVICE_ACCOUNT_EMAIL = os.environ.get("SERVICE_ACCOUNT_EMAIL")

# Initialize default client (will be overridden if impersonation is needed)
_default_client = storage.Client()


def upload_file(bucket_name, path, local_path):
    """Upload a local file to GCS."""
    bucket = _default_client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{path}"


def upload_bytes(bucket_name, path, data: bytes):
    """Upload raw bytes to GCS."""
    bucket = _default_client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(data)
    return f"gs://{bucket_name}/{path}"


def download_file(bucket_name, path, local_path):
    """Download GCS file to local path."""
    bucket = _default_client.bucket(bucket_name)
    blob = bucket.blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{path} not found")
    blob.download_to_filename(local_path)
    return local_path


def download_bytes(bucket_name, path):
    """Download GCS file contents as bytes."""
    bucket = _default_client.bucket(bucket_name)
    blob = bucket.blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{path} not found")
    return blob.download_as_bytes()


def merge_csvs(bucket_name, input_paths, output_path):
    """Merge multiple CSV files in GCS into one final CSV."""
    frames = []
    for p in input_paths:
        bucket = _default_client.bucket(bucket_name)
        blob = bucket.blob(p)
        if not blob.exists():
            continue
        data = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(data))
        frames.append(df)

    if not frames:
        raise ValueError("No CSVs found to merge")

    merged = pd.concat(frames)
    bucket = _default_client.bucket(bucket_name)
    blob = bucket.blob(output_path)
    blob.upload_from_string(merged.to_csv(index=False), content_type="text/csv")
    return f"gs://{bucket_name}/{output_path}"


def generate_signed_url(bucket_name, blob_name, expiration=3600):
    """
    Generate a V4 signed URL for a blob inside Cloud Run.
    Requires roles:
      - roles/storage.objectAdmin
      - roles/iam.serviceAccountTokenCreator
    """

    creds, _ = default()

    if SERVICE_ACCOUNT_EMAIL:
        # Impersonate the service account to get signing power
        target_creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=SERVICE_ACCOUNT_EMAIL,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=3600,
        )
        client = storage.Client(credentials=target_creds)
    else:
        client = _default_client

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(seconds=expiration),
        method="GET",
    )
    return url
