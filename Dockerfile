# TriageIQ console in demo mode: simulated tickets and AI results, no email, no phone alerts,
# no secrets needed. The full system (Kafka, Qdrant, Ollama, Gmail) runs on your own machine;
# see the README.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN useradd --system --uid 1001 triageiq && chown -R triageiq /app
USER triageiq

# TRIAGEIQ_HOSTED=1: demo data only, listens on 0.0.0.0, port from $PORT
ENV TRIAGEIQ_HOSTED=1 PORT=8050 PYTHONUNBUFFERED=1
EXPOSE 8050
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request,os;urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/api/health')"
CMD ["python", "dashboard/server.py"]
