"""
TriageIQ - Step 4a: load past resolved tickets into the vector database.

Run once (and again whenever you edit past_tickets.json):
    python rag/ingest.py
"""

import json
from pathlib import Path

import requests

import store

DATA_FILE = Path(__file__).parent / "past_tickets.json"


def main():
    tickets = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    print(f"Loaded {len(tickets)} past tickets from {DATA_FILE.name}")

    try:
        print(f"Turning them into embeddings with '{store.EMBED_MODEL}' ...")
        vectors = store.embed([t["question"] for t in tickets], "document")

        store.recreate_collection(len(vectors[0]))
        points = [
            {"id": i + 1, "vector": vector, "payload": ticket}
            for i, (ticket, vector) in enumerate(zip(tickets, vectors))
        ]
        store.upsert(points)
        print(f"Stored {store.count()} past tickets in Qdrant collection '{store.COLLECTION}'")
        print(f"Each ticket became a list of {len(vectors[0])} numbers.")
    except requests.exceptions.ConnectionError:
        print("Could not connect. Check that:")
        print("  - Ollama is running (llama icon in the system tray)")
        print("  - Qdrant is running (docker ps shows a row named qdrant)")
    except requests.exceptions.HTTPError as exc:
        print(f"A service answered with an error: {exc.response.text}")
        print("If it says the model was not found, run:  ollama pull nomic-embed-text")


if __name__ == "__main__":
    main()