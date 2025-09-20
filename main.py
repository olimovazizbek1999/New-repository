from fastapi import FastAPI
import os

app = FastAPI()

# Cloud Run sets this environment variable
PORT = int(os.environ.get("PORT", 8080))

@app.get("/")
def read_root():
    return {"message": "Hello, Cloud Run!"}

# Only run locally with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
