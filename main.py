import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()
PORT = int(os.environ.get("PORT", 8080))

@app.get("/")
def read_root():
    return {"message": "Hello, Cloud Run!"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
