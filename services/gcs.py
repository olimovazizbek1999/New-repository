import io
if not b.exists():
raise FileNotFoundError(f"gs://{bucket}/{path} not found")
fd, temp_path = tempfile.mkstemp()
os.close(fd)
b.download_to_filename(temp_path)
return temp_path




def write_json(bucket: str, path: str, obj: Dict):
data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
upload_bytes(bucket, path, data, content_type="application/json")




def read_json(bucket: str, path: str) -> Optional[Dict]:
b = _client().bucket(bucket).blob(path)
if not b.exists():
return None
s = b.download_as_text()
return json.loads(s)




def list_paths(bucket: str, prefix: str) -> List[str]:
blobs = _client().bucket(bucket).list_blobs(prefix=prefix)
return [bl.name for bl in blobs]




def generate_signed_url(bucket: str, path: str, days: int = 7) -> str:
blob = _client().bucket(bucket).blob(path)
url = blob.generate_signed_url(expiration=timedelta(days=days), method="GET")
return url




def write_df_csv(bucket: str, path: str, df, include_header: bool = True):
# Stream to temp file to avoid memory bloat
tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
df.to_csv(tmp.name, index=False, header=include_header)
with open(tmp.name, "rb") as f:
upload_fileobj(bucket, path, f, content_type="text/csv")




def merge_chunks_to_final(bucket: str, job_id: str, original_filename: str) -> str:
out_prefix = f"jobs/{job_id}/out/"
chunk_paths = sorted(
[p for p in list_paths(bucket, out_prefix) if p.endswith('.csv')],
key=lambda p: int(p.split("chunk_")[-1].split(".")[0])
)
if not chunk_paths:
raise RuntimeError("No chunk outputs found to merge")


# Write merged to temp file then upload once
header_written = False
merged_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
for p in chunk_paths:
tmp = download_to_tempfile(bucket, p)
with open(tmp, "rb") as f_in:
data = f_in.read()
text = data.decode("utf-8")
lines = text.splitlines()
if not lines:
continue
if header_written:
content = "\n".join(lines[1:]) + "\n"
else:
content = "\n".join(lines) + "\n"
header_written = True
with open(merged_tmp.name, "ab") as f_out:
f_out.write(content.encode("utf-8"))


final_name = original_filename.rsplit(".", 1)[0] + "_processed.csv"
final_path = f"jobs/{job_id}/final/{final_name}"
with open(merged_tmp.name, "rb") as f:
upload_fileobj(bucket, final_path, f, content_type="text/csv")
logger.info(f"Merged {len(chunk_paths)} chunks into gs://{bucket}/{final_path}")
return final_path