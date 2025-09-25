import os
import datetime
from google.cloud import storage
from google.auth import default, impersonated_credentials
from tenacity import retry, stop_after_attempt, wait_exponential

# Optional impersonation target
SERVICE_ACCOUNT_EMAIL = os.environ.get("SERVICE_ACCOUNT_EMAIL")

# Lazy client
_default_client = None


def get_client():
    """Return a cached Google Cloud Storage client."""
    global _default_client
    if _default_client is None:
        _default_client = storage.Client()
    return _default_client


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def upload_bytes(bucket_name: str, path: str, data: bytes):
    """Upload raw bytes to GCS."""
    bucket = get_client().bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(data)
    print(f"✅ Uploaded gs://{bucket_name}/{path}")
    return f"gs://{bucket_name}/{path}"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def download_bytes(bucket_name: str, path: str) -> bytes:
    """Download GCS file contents as bytes."""
    bucket = get_client().bucket(bucket_name)
    blob = bucket.blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{path} not found")
    return blob.download_as_bytes()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def merge_csvs(bucket_name: str, input_paths: list[str], output_path: str) -> str:
    """
    Merge multiple CSV files in GCS into one final CSV using compose.
    Much faster than downloading + Pandas concat.
    """
    client = get_client()
    bucket = client.bucket(bucket_name)
    blobs = [bucket.blob(p) for p in input_paths]
    target = bucket.blob(output_path)

    if not blobs:
        raise ValueError("No input CSVs to merge")

    # Compose max 32 at a time
    while len(blobs) > 32:
        part = blobs[:32]
        tmp_blob = bucket.blob(output_path + ".tmp")
        tmp_blob.compose(part)
        blobs = [tmp_blob] + blobs[32:]

    target.compose(blobs)

    # ✅ Cleanup: delete original chunk files after merge
    for p in input_paths:
        try:
            bucket.blob(p).delete()
            print(f"🗑️ Deleted temp chunk {p}")
        except Exception as e:
            print(f"⚠️ Could not delete {p}: {e}")

    print(f"✅ Composed {len(input_paths)} chunks → gs://{bucket_name}/{output_path}")
    return f"gs://{bucket_name}/{output_path}"


def generate_signed_url(bucket_name: str, blob_name: str, expiration=3600) -> str:
    """
    Generate a V4 signed URL for a blob inside Cloud Run.
    Requires roles:
      - roles/storage.objectAdmin
      - roles/iam.serviceAccountTokenCreator
    """
    creds, _ = default()

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
    print(f"✅ Generated signed URL for gs://{bucket_name}/{blob_name}")
    return url
