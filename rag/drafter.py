"""
TriageIQ - Step 4b: draft a reply to a ticket using similar past tickets (RAG).

R = Retrieve : find the past tickets closest in meaning to the new one (Qdrant)
A = Augment  : paste those past answers into the prompt
G = Generate : the LLM writes a reply using ONLY those answers

Quick test:
    python rag/drafter.py
"""

import time

import requests

import store

OLLAMA_CHAT_URL = "http://localhost:11434/api/chat"
CHAT_MODEL = "qwen2.5:3b"
TIMEOUT_SECONDS = 180

TOP_K = 3             # how many past tickets to look at
MIN_SIMILARITY = 0.55  # below this, a past ticket is not similar enough to trust (tune this later)

SYSTEM_PROMPT = """You are a polite customer support agent.
Write a short reply (3 to 5 sentences) to the customer's message.

Rules:
- Use ONLY the facts in the "how it was solved" answers you are given.
- Never invent policies, amounts, links, or time frames that are not in them.
- If those answers do not fully answer the customer, say a team member will follow up.
- Do not mention past tickets or ticket IDs.
- Start with a brief apology or acknowledgement when the customer has a problem.
- End with "Support Team"."""


def _ask_llm(messages: list[dict]) -> str:
    payload = {
        "model": CHAT_MODEL,
        "messages": messages,
        "stream": False,
        "options": {"temperature": 0.2, "num_predict": 220},  # num_predict caps the length
    }
    response = requests.post(OLLAMA_CHAT_URL, json=payload, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()["message"]["content"].strip()


def draft_reply(message: str) -> dict:
    """
    Never raises. Returns:
        {"reply": text or None, "sources": [past tickets used], "error": reason or None}
    A reply of None means: a human should answer this one.
    """
    # 1) Retrieve
    try:
        vector = store.embed([message], "query")[0]
        hits = store.search(vector, limit=TOP_K)
    except requests.exceptions.ConnectionError:
        return {"reply": None, "sources": [], "error": "Ollama or Qdrant is not running"}
    except Exception as exc:
        return {"reply": None, "sources": [], "error": f"Search failed: {exc}"}

    sources = [
        {"id": h["payload"]["id"], "score": round(h["score"], 2), "question": h["payload"]["question"]}
        for h in hits
    ]
    relevant = [h for h in hits if h["score"] >= MIN_SIMILARITY]
    if not relevant:
        return {"reply": None, "sources": sources, "error": "No similar past ticket found, a human should answer"}

    # 2) Augment
    examples = "\n\n".join(
        f"{i}. Past question: {h['payload']['question']}\n   How it was solved: {h['payload']['resolution']}"
        for i, h in enumerate(relevant, 1)
    )
    user_text = (
        f"Customer message:\n{message}\n\n"
        f"Similar past tickets and how they were solved:\n{examples}\n\n"
        "Write the reply now."
    )

    # 3) Generate
    try:
        reply = _ask_llm([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ])
    except requests.exceptions.Timeout:
        return {"reply": None, "sources": sources, "error": "LLM timed out"}
    except Exception as exc:
        return {"reply": None, "sources": sources, "error": f"LLM call failed: {exc}"}

    if not reply:
        return {"reply": None, "sources": sources, "error": "LLM returned an empty reply"}
    return {"reply": reply, "sources": sources, "error": None}


if __name__ == "__main__":
    samples = [
        "I was charged twice for my Pro subscription and I need a refund today!!",
        "Someone logged in to my account from another country! Please secure it immediately.",
        "What is the capital of France?",
    ]
    for text in samples:
        start = time.time()
        result = draft_reply(text)
        seconds = time.time() - start
        print(f"\nTicket : {text}")
        print("Used   : " + (", ".join(f"{s['id']} ({s['score']})" for s in result["sources"]) or "nothing"))
        if result["reply"]:
            print(f"Reply  : {result['reply']}")
        else:
            print(f"No draft: {result['error']}")
        print(f"Time   : {seconds:.1f}s")