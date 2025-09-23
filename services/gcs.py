import os
import datetime
import pandas as pd
from google.cloud import storage
from google.auth import default, impersonated_credentials

# Read service account email from env
SERVICE_ACCOUNT_EMAIL = os.environ.get("SERVICE_ACCOUNT_EMAIL")

# Lazy client
_default_client = None


def get_client():
    """Return a cached Google Cloud Storage client."""
    global _default_client
    if _default_client is None:
        _default_client = storage.Client()
    return _default_client


def upload_bytes(bucket_name, path, data: bytes):
    """Upload raw bytes to GCS."""
    bucket = get_client().bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(data)
    print(f"Uploaded gs://{bucket_name}/{path}")
    return f"gs://{bucket_name}/{path}"


def download_bytes(bucket_name, path):
    """Download GCS file contents as bytes."""
    bucket = get_client().bucket(bucket_name)
    blob = bucket.blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{path} not found")
    return blob.download_as_bytes()


def merge_csvs(bucket_name, input_paths, output_path):
    """Merge multiple CSV files in GCS into one final CSV."""
    frames = []
    for p in input_paths:
        bucket = get_client().bucket(bucket_name)
        blob = bucket.blob(p)
        if not blob.exists():
            continue
        data = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(data))
        frames.append(df)

    if not frames:
        raise ValueError("No CSVs found to merge")

    merged = pd.concat(frames)
    bucket = get_client().bucket(bucket_name)
    blob = bucket.blob(output_path)
    blob.upload_from_string(merged.to_csv(index=False), content_type="text/csv")
    print(f"Merged {len(frames)} chunks into gs://{bucket_name}/{output_path}")
    return f"gs://{bucket_name}/{output_path}"


def generate_signed_url(bucket_name, blob_name, expiration=3600):
    """
    Generate a V4 signed URL for a blob inside Cloud Run.
    Requires roles:
      - roles/storage.objectAdmin
      - roles/iam.serviceAccountTokenCreator
    """
    creds, _ = default()

    # ✅ Use impersonated service account if provided
    if SERVICE_ACCOUNT_EMAIL:
        target_creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=SERVICE_ACCOUNT_EMAIL,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=3600,
        )
        client = storage.Client(credentials=target_creds)
    else:
        client = get_client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(seconds=expiration),
        method="GET",
    )
    print(f"Generated signed URL for gs://{bucket_name}/{blob_name}")
    return url
