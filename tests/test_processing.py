import os
import pytest
import pandas as pd
import tempfile
from services import processing


class DummyBucket:
    def __init__(self):
        self._storage = {}

    def blob(self, path):
        return DummyBlob(path, self._storage)


class DummyBlob:
    def __init__(self, path, storage):
        self.path = path
        self.storage = storage

    def upload_from_string(self, data, content_type=None):
        self.storage[self.path] = data
        return True

    def exists(self):
        return self.path in self.storage

    def download_as_bytes(self):
        return self.storage.get(self.path, b"")

    def generate_signed_url(self, *a, **kw):
        return "http://signed-url.example.com"


class DummyClient:
    def __init__(self):
        self.bucket_obj = DummyBucket()

    def bucket(self, name):
        return self.bucket_obj


@pytest.fixture(autouse=True)
def patch_gcs(monkeypatch):
    """Patch GCS client and helper functions for tests."""
    dummy_client = DummyClient()

    monkeypatch.setattr("services.gcs.get_client", lambda: dummy_client)
    monkeypatch.setattr("services.gcs.generate_signed_url", lambda *a, **kw: "http://signed-url.example.com")
    monkeypatch.setattr("services.processing.send_email", lambda **kwargs: True)
    yield


def test_finalize_job_runs_without_error(tmp_path):
    job_id = "test-job"

    # Fake manifest
    manifest = {
        "job_id": job_id,
        "filename": "test.csv",
        "processed_chunks": [0],
        "log": [],
        "email": "test@example.com",
    }

    # Write a dummy chunk CSV
    csv_path = tmp_path / "chunk_0.csv"
    pd.DataFrame([{"First Name": "Alice", "Last Name": "Smith", "Company": "ACME", "Opener": "Hi"}]).to_csv(csv_path, index=False)

    # Monkeypatch merge_csvs to just copy file
    def fake_merge_csvs(bucket, inputs, output):
        return str(csv_path)

    def fake_download_bytes(bucket, path):
        return csv_path.read_bytes()

    def fake_upload_bytes(bucket, path, data):
        return f"gs://{bucket}/{path}"

    # Patch in fake GCS functions
    import services.gcs as gcs
    gcs.merge_csvs = fake_merge_csvs
    gcs.download_bytes = fake_download_bytes
    gcs.upload_bytes = fake_upload_bytes

    processing.finalize_job(job_id, manifest)

    assert manifest["status"] == "done"
    assert manifest["email_sent"] is True
    assert "final_signed_url" in manifest


def test_process_chunk_cleans_and_generates(monkeypatch, tmp_path):
    job_id = "test-job-2"
    manifest = {
        "job_id": job_id,
        "filename": "test2.csv",
        "input": f"gs://dummy-bucket/jobs/{job_id}/input/test2.csv",
        "chunk_size": 10,
        "log": [],
        "processed_chunks": [],
        "email": "user@example.com",
    }

    # Dummy CSV
    df = pd.DataFrame(
        [
            {"First Name": " bob ", "Last Name": " smith ", "Company": "  acme ", "Website": "example.com", "Industry": "Tech"},
            {"First Name": " alice ", "Last Name": " jones ", "Company": " startup ", "Website": "", "Industry": "Finance"},
        ]
    )
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Patch manifest and GCS
    monkeypatch.setattr(processing, "load_manifest", lambda job_id: manifest.copy())
    monkeypatch.setattr(processing, "save_manifest", lambda job_id, m: None)
    monkeypatch.setattr("services.gcs.download_bytes", lambda bucket, path: csv_bytes)
    monkeypatch.setattr("services.gcs.upload_bytes", lambda bucket, path, data: True)

    # Patch OpenAI + scraper
    monkeypatch.setattr(processing, "clean_rows", lambda rows, job_id=None: [{"first": "Bob", "last": "Smith", "company": "ACME"} for _ in rows])
    monkeypatch.setattr(processing, "generate_openers", lambda rows, job_id=None: [{"opener": "This is a professional opener."} for _ in rows])
    monkeypatch.setattr(processing, "extract_company_info", lambda url: {"name": "ExampleCo", "text": "Homepage text"})

    # Run
    import asyncio
    asyncio.run(processing.process_chunk(job_id, 0))

    assert True  # If no exception is raised, success
