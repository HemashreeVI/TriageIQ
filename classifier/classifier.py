"""
TriageIQ - Step 3: LLM classifier (runs locally with Ollama).

Reads ONE ticket message and returns:
    category  (Billing / Technical / Delivery / Account / General)
    priority  (High / Medium / Low)
    mood      (Angry / Frustrated / Neutral / Happy)

If the LLM is down, slow, or returns something invalid, we do NOT crash.
We return a safe result with needs_human = True so a person looks at it.

Run a quick test:
    python classifier/classifier.py
"""

import json
import time

import requests

OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL = "qwen2.5:3b"
TIMEOUT_SECONDS = 120  # the very first call is slow while the model loads

CATEGORIES = ["Billing", "Technical", "Delivery", "Account", "General"]
PRIORITIES = ["High", "Medium", "Low"]
MOODS = ["Angry", "Frustrated", "Neutral", "Happy"]

# This schema forces the LLM to answer in exactly this JSON shape.
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": CATEGORIES},
        "priority": {"type": "string", "enum": PRIORITIES},
        "mood": {"type": "string", "enum": MOODS},
    },
    "required": ["category", "priority", "mood"],
}

SYSTEM_PROMPT = """You are a support ticket triage assistant.
Read the customer's message and classify it.

category (pick exactly one):
- Billing: payments, charges, double charges, refunds, invoices
- Technical: app errors, crashes, login ERROR codes, slow pages, how-to questions about using the product
- Delivery: orders, shipping, tracking, wrong or missing items
- Account: password reset, profile or phone number changes, deleting an account, and ANY security problem such as someone else logging in, a hacked account, or suspicious activity. Security problems are ALWAYS Account, never Technical.
- General: anything else (discounts, thanks, praise, questions about features or plans)

priority:
- High: money lost or wrongly charged, customer cannot use the product at all, a security risk, or the customer is very upset
- Medium: a real problem that is not blocking the customer right now
- Low: simple questions (including "where is my order" or "how do I track it" with no complaint), thanks, or requests with no urgency

mood: Angry (shouting, threats, very upset), Frustrated (annoyed or worried), Neutral, or Happy.

Answer only with JSON."""


def safe_fallback(reason: str) -> dict:
    """What we return when the LLM fails: a human must handle this ticket."""
    return {
        "category": "Unknown",
        "priority": "High",  # unknown tickets are treated carefully, not ignored
        "mood": "Neutral",
        "needs_human": True,
        "error": reason,
    }


def validate(result: dict) -> bool:
    """Check the LLM's answer really contains valid values."""
    return (
        isinstance(result, dict)
        and result.get("category") in CATEGORIES
        and result.get("priority") in PRIORITIES
        and result.get("mood") in MOODS
    )


def classify_ticket(message: str) -> dict:
    """Classify one ticket message. Never raises: always returns a dict."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": message},
        ],
        "stream": False,
        "format": RESPONSE_SCHEMA,
        "options": {"temperature": 0},  # 0 = same answer every time
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        content = response.json()["message"]["content"]
        result = json.loads(content)
    except requests.exceptions.ConnectionError:
        return safe_fallback("Ollama is not running")
    except requests.exceptions.Timeout:
        return safe_fallback("LLM timed out")
    except Exception as exc:  # bad JSON, HTTP error, anything unexpected
        return safe_fallback(f"LLM call failed: {exc}")

    if not validate(result):
        return safe_fallback(f"Invalid LLM output: {result}")

    result["needs_human"] = False
    return result


if __name__ == "__main__":
    samples = [
        "I was charged twice for my Pro subscription and I need a refund today!!",
        "Where can I track my package ORD-48213?",
        "Someone logged in to my account from another country! Please secure it immediately.",
        "Just wanted to say thank you, the support team was really helpful!",
        "The Dashboard page is loading very slowly since yesterday.",
    ]
    print(f"Using model: {MODEL}\n")
    for text in samples:
        start = time.time()
        result = classify_ticket(text)
        seconds = time.time() - start
        print(f"Ticket : {text}")
        print(f"Result : {result}")
        print(f"Time   : {seconds:.1f}s\n")