import os
import json
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from openai import OpenAI

# Load OpenAI client
_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

class OpenAIError(Exception):
    """Custom error wrapper for OpenAI issues"""
    pass


# 🔹 Retry wrapper for resiliency
def _retry_decorator(func):
    return retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(Exception),
    )(func)


@_retry_decorator
def clean_rows(rows):
    """
    Cleans names and company fields using OpenAI.
    Input: list of dicts, e.g. [{"first":" John ", "last":"Doe", "company":"Acme"}]
    Output: JSON string with cleaned values
    """
    response = _client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a data cleaning assistant. "
                    "Always reply strictly in valid JSON."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Clean the following rows (trim whitespace, remove symbols). "
                    "Return output strictly as JSON array with the same keys.\n\n"
                    + json.dumps(rows)
                ),
            },
        ],
        response_format={"type": "json_object"},
    )

    return response.choices[0].message.content


@_retry_decorator
def generate_openers(rows):
    """
    Generates professional two-sentence openers.
    Input: list of dicts, each with company website text etc.
    Output: JSON string with added 'opener' key.
    """
    response = _client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an assistant that generates professional, calm, specific email openers. "
                    "You must always respond in valid JSON."
                ),
            },
            {
                "role": "user",
                "content": (
                    "For each row, generate a two-sentence email opener. "
                    "Style: professional, calm, neutral-positive. "
                    "Length: each sentence 10–18 words. "
                    "Second sentence must flow naturally and end cleanly (no sales pitch). "
                    "Return output strictly as JSON array with the same keys plus an 'opener' field.\n\n"
                    + json.dumps(rows)
                ),
            },
        ],
        response_format={"type": "json_object"},
    )

    return response.choices[0].message.content
