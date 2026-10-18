"""
TriageIQ - Step 4a: helpers for the vector database (Qdrant) and embeddings (Ollama).

An EMBEDDING turns a sentence into a list of numbers that captures its meaning.
Sentences with similar meaning get similar numbers, so we can search by meaning.
"""

import requests

OLLAMA_EMBED_URL = "http://localhost:11434/api/embed"
EMBED_MODEL = "nomic-embed-text"

QDRANT_URL = "http://localhost:6333"
COLLECTION = "past_tickets"


def embed(texts: list[str], kind: str) -> list[list[float]]:
    """Turn texts into embeddings. kind is 'document' (stored) or 'query' (searched)."""
    # nomic-embed-text works best when told whether the text is stored or searched
    inputs = [f"search_{kind}: {t}" for t in texts]
    response = requests.post(
        OLLAMA_EMBED_URL, json={"model": EMBED_MODEL, "input": inputs}, timeout=120
    )
    response.raise_for_status()
    return response.json()["embeddings"]


def recreate_collection(dimension: int):
    """Create an empty collection (like a table) for our vectors. Deletes the old one."""
    requests.delete(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=30)
    response = requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}",
        json={"vectors": {"size": dimension, "distance": "Cosine"}},
        timeout=30,
    )
    response.raise_for_status()


def upsert(points: list[dict]):
    """Store points. Each point = {'id': int, 'vector': [...], 'payload': {...}}."""
    response = requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points?wait=true",
        json={"points": points},
        timeout=60,
    )
    response.raise_for_status()


def count() -> int:
    response = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=30)
    response.raise_for_status()
    return response.json()["result"]["points_count"]


def search(vector: list[float], limit: int = 3) -> list[dict]:
    """Find the stored points whose meaning is closest to the vector."""
    response = requests.post(
        f"{QDRANT_URL}/collections/{COLLECTION}/points/search",
        json={"vector": vector, "limit": limit, "with_payload": True},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["result"]