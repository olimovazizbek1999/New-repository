import openai
import os
from tenacity import retry, stop_after_attempt, wait_exponential

openai.api_key = os.environ.get("OPENAI_API_KEY")

@retry(stop=stop_after_attempt(10), wait=wait_exponential(min=1, max=10))
def clean_names(batch):
    # Implement batch cleaning via OpenAI
    pass

@retry(stop=stop_after_attempt(10), wait=wait_exponential(min=1, max=10))
def generate_openers(batch):
    # Implement batch email opener generation via OpenAI
    pass
