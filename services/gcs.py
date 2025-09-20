from google.cloud import storage
import json

def upload_fileobj(bucket_name, destination_path, fileobj):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_file(fileobj)

def upload_json(bucket_name, destination_path, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
