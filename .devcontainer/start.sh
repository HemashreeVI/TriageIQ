#!/usr/bin/env bash
# Starts the TriageIQ pipeline in a Codespace / dev container, without any email account.
# Logs: /tmp/triageiq-*.log
set -euo pipefail
cd "$(dirname "$0")/.."

# Docker comes with the Codespaces image; give its daemon a moment to start.
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ Docker isn't available in this container."
  echo "   Command Palette (Ctrl/Cmd+Shift+P) → 'Codespaces: Rebuild Container', then run: bash .devcontainer/start.sh"
  exit 1
fi
for _ in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 2; done

say() { echo "▶ $*"; }

say "Python packages"
python3 -m pip install --quiet --disable-pip-version-check -r requirements.txt

say "Kafka and Qdrant"
docker compose up -d

say "Local LLM (Ollama)"
docker start ollama >/dev/null 2>&1 || docker run -d --name ollama -p 11434:11434 -v ollama:/root/.ollama ollama/ollama >/dev/null
until curl -fsS http://localhost:11434/api/tags >/dev/null 2>&1; do sleep 2; done
for model in qwen2.5:3b nomic-embed-text; do
  say "  model $model (first time only: a few minutes)"
  docker exec ollama ollama pull "$model" >/dev/null
done

say "Kafka topics"
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
for topic in tickets tickets.triaged; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$topic" \
    --bootstrap-server localhost:9092 >/dev/null
done

say "Past tickets → vector database"
python3 rag/ingest.py > /tmp/triageiq-ingest.log 2>&1

[ -f .env ] || cp .env.example .env          # no email needed; the console fills in its login

say "Classifier + console"
pkill -f "classifier/consumer.py" 2>/dev/null || true
pkill -f "dashboard/server.py" 2>/dev/null || true
nohup python3 classifier/consumer.py --max 0 > /tmp/triageiq-classifier.log 2>&1 &
nohup python3 dashboard/server.py > /tmp/triageiq-console.log 2>&1 &
until curl -fsS http://localhost:8050/api/health >/dev/null 2>&1; do sleep 2; done

say "Fake customers: 20 support tickets"
nohup python3 producer/ticket_producer.py --count 20 --delay 3 > /tmp/triageiq-producer.log 2>&1 &

echo
echo "✅ TriageIQ is running → Ports tab → 8050 (TriageIQ console)"
grep -m1 -E "Sign in with|Sign in as" /tmp/triageiq-console.log || true
echo "   Login details are also in .env (DASHBOARD_USER / DASHBOARD_PASSWORD)."
echo "   Logs: /tmp/triageiq-classifier.log, /tmp/triageiq-console.log"
