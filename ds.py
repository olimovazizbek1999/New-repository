client = storage.Client()
for bucket in client.list_buckets():
    print(bucket.name)
