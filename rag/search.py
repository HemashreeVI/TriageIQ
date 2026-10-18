"""
TriageIQ - Step 4a: search past tickets BY MEANING.

Try your own sentence:
    python rag/search.py "I was billed two times for my plan"

Or run without text to try 4 built-in examples:
    python rag/search.py
"""

import sys

import requests

import store

DEMO_QUERIES = [
    "I got billed two times for my plan",
    "my parcel still has not shown up",
    "somebody else is using my login from abroad",
    "how can I get a receipt for tax purposes",
]


def show(query: str):
    vector = store.embed([query], "query")[0]
    hits = store.search(vector, limit=3)
    print(f'\nNew ticket: "{query}"')
    for rank, hit in enumerate(hits, 1):
        past = hit["payload"]
        print(f"  {rank}. similarity {hit['score']:.2f} | {past['category']} | past ticket: {past['question']}")
        if rank == 1:
            print(f"     how we solved it: {past['resolution']}")


def main():
    queries = [" ".join(sys.argv[1:])] if len(sys.argv) > 1 else DEMO_QUERIES
    try:
        for query in queries:
            show(query)
    except requests.exceptions.ConnectionError:
        print("Could not connect. Is Ollama running, and does docker ps show qdrant?")
    except requests.exceptions.HTTPError as exc:
        print(f"A service answered with an error: {exc.response.text}")
        print("If the collection is missing, run:  python rag/ingest.py")


if __name__ == "__main__":
    main()