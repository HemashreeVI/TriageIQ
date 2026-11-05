<p align="center">
  <img src="./assets/readme/hero.svg" width="100%" alt="TriageIQ: Support emails triaged in seconds. A human approves every reply.">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-F4A73B?style=for-the-badge" alt="Python"> <img src="https://img.shields.io/badge/Kafka-F4A73B?style=for-the-badge" alt="Kafka"> <img src="https://img.shields.io/badge/Ollama-F4A73B?style=for-the-badge" alt="Ollama"> <img src="https://img.shields.io/badge/Qdrant-F4A73B?style=for-the-badge" alt="Qdrant"> <img src="https://img.shields.io/badge/FastAPI-F4A73B?style=for-the-badge" alt="FastAPI"> <img src="https://img.shields.io/badge/Docker-F4A73B?style=for-the-badge" alt="Docker">
</p>

<img src="./assets/readme/title-features.svg" width="100%" alt="01 Features: What it does.">
<img src="./assets/readme/features.svg" width="100%" alt="TriageIQ features">

<img src="./assets/readme/title-flow.svg" width="100%" alt="02 Pipeline: From inbox to reply.">
<img src="./assets/readme/flow.svg" width="100%" alt="Receive → Queue → Classify → Retrieve → Draft → Approve">

<img src="./assets/readme/title-table.svg" width="100%" alt="03 The Console: Built for the support team.">
<img src="./assets/readme/table.svg" width="100%" alt="The Console">

<img src="./assets/readme/title-stack.svg" width="100%" alt="04 Tech stack: The tools behind it.">
<img src="./assets/readme/stack.svg" width="100%" alt="TriageIQ tech stack">

<img src="./assets/readme/title-run.svg" width="100%" alt="05 Run it: Try it yourself.">

## 📸 Screenshots

From the console in demo mode (`python dashboard/server.py --demo`). **Demo tickets and AI results are simulated**: nothing is emailed, and the numbers are not real model accuracy.

| | |
|---|---|
| <img src="assets/screenshots/1-overview.png" alt="Overview"> | <img src="assets/screenshots/2-approval-queue.png" alt="Approval queue"> |
| **Overview.** How many tickets are waiting, split by priority, and how long the oldest has waited. | **Needs approval.** Each ticket shows its category, priority and mood, the customer's message and the AI's draft. |
| <img src="assets/screenshots/3-ticket-and-draft.png" alt="Ticket and AI draft"> | <img src="assets/screenshots/4-urgent.png" alt="Urgent tickets"> |
| **Ticket and draft.** Read it, change anything, then approve or send it to a human. Nothing is sent until you do. | **Urgent.** High-priority tickets, and the tickets the AI could not handle. |
| <img src="assets/screenshots/5-all-tickets.png" alt="All tickets"> | <img src="assets/screenshots/6-system.png" alt="System page"> |
| **All tickets.** Every ticket, searchable and filterable by source and status. | **System.** How a ticket travels through the pipeline, with live counters for each stage. |

## 🧠 How it works

<img src="assets/explainer/1-pipeline.png" alt="The event-driven pipeline" width="100%">

```mermaid
flowchart LR
    IN[📥 Inbox] -->|IMAP| GW[gateway/inbound.py]
    GW -->|new ticket| K1[(Kafka: tickets)]
    K1 --> CL[classifier/consumer.py<br/>classify + RAG draft]
    CL <-->|qwen2.5:3b, nomic-embed-text| OL[Ollama]
    CL <-->|top 3 similar past tickets| QD[(Qdrant)]
    CL --> K2[(Kafka: tickets.triaged)]
    K2 --> UI[FastAPI console<br/>live over SSE]
    UI -->|approve / edit / escalate| AG[🧑 support agent]
    AG -->|only after approval| SM[SMTP reply<br/>same thread]
```

### Why Kafka in the middle

<img src="assets/explainer/2-why-kafka.png" alt="Why Kafka" width="100%">

Kafka decouples reading email from running the model. If the model is slow, tickets wait safely in the `tickets` topic instead of being lost. The consumer group `triageiq-classifier` stores its offset, so after a restart it carries on from exactly where it stopped.

### Step 1: classify with a local LLM

<img src="assets/explainer/3-classify.png" alt="Classify with a local LLM" width="100%">

- The model is **Qwen 2.5 3B** (`qwen2.5:3b`) running locally in **Ollama**. Customer text never leaves the machine.
- The prompt defines every category and priority, and a **JSON schema** forces exactly three fields (`category`, `priority`, `mood`), each from a fixed list.
- **Temperature 0**, so the same email always gets the same answer.

### Never trust the model blindly

<img src="assets/explainer/4-safe-fallback.png" alt="Safe fallback" width="100%">

If Ollama is down, times out, or returns anything outside the allowed values, the ticket is **not dropped**. It falls back to `needs_human: True` with **High** priority, so a person looks at it first. The terminal on the right is a real run with the model switched off.

### Step 2: draft a reply with RAG

<img src="assets/explainer/5-rag.png" alt="RAG over solved tickets" width="100%">

```mermaid
sequenceDiagram
    participant C as consumer
    participant O as Ollama
    participant Q as Qdrant
    C->>O: embed the new ticket (nomic-embed-text)
    O-->>C: vector
    C->>Q: 3 nearest past tickets (cosine)
    Q-->>C: matches with similarity scores
    alt at least one match ≥ 0.55
        C->>O: draft a reply using only those past answers
        O-->>C: draft + sources
    else nothing similar enough
        Note over C: no draft, a human writes the reply
    end
```

- 30 past tickets and how they were solved are embedded with `nomic-embed-text` and stored in Qdrant (`rag/ingest.py`).
- A new ticket is embedded the same way, and Qdrant returns the **top 3** by **cosine similarity**.
- Anything below **0.55** is ignored. If nothing is similar enough, there is no draft.

### Grounded replies

<img src="assets/explainer/6-grounded.png" alt="Grounded replies" width="100%">

The drafting prompt says: use only the facts in the past answers; never invent policies, amounts, links or time frames; if they don't fully answer the customer, say a team member will follow up. The model writes the words, but the facts come from your own history. The console shows which past tickets each draft was based on.

### Step 3: a human stays in the loop

<img src="assets/explainer/7-human-in-the-loop.png" alt="Human in the loop" width="100%">

The console streams every ticket over **Server-Sent Events** (`/events`). The agent can **approve** the draft, **edit** it, **write their own**, or **send it to a human**. Only on approval does the reply go out over SMTP, in the same thread, using the `In-Reply-To` and `References` headers.

### Hardened for a real inbox

<img src="assets/explainer/8-hardening.png" alt="Hardening" width="100%">

- **No duplicates:** every email's `Message-ID` is stored in SQLite, so one email is one ticket, even after restarts.
- **No reply loops:** auto-replies (`Auto-Submitted`), bulk mail (`Precedence: bulk`), mailing lists (`List-Id`), bounces and no-reply senders are skipped.
- **Rate limit:** one sender can create at most **5 tickets per hour** (`MAX_PER_SENDER_PER_HOUR`).
- **Sign-in:** an HMAC-signed 14-day session cookie, and a lockout after **6 wrong passwords in 10 minutes**.


## Project layout

```
TriageIQ/
  docker-compose.yml        Kafka and Qdrant
  producer/                 generates fake tickets for testing
  gateway/                  email in and out, settings, phone alerts
  classifier/               LLM classifier and the Kafka consumer
  rag/                      past tickets, Qdrant search, reply drafting
  dashboard/                FastAPI server, sign-in, console and app files
  .env.example              settings template (copy to .env)
```


## 🌐 Hosted demo

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/HemashreeVI/TriageIQ)

`render.yaml` runs the console in demo mode with `TRIAGEIQ_HOSTED=1`: simulated tickets and AI results, no Kafka or local model needed, and it never reads `.env`, sends email or sends phone alerts. The real pipeline (IMAP, Kafka, Ollama, Qdrant) runs on your own machine, see Quick start.

## Quick start

**Requirements:** Docker Desktop, Python 3.11 or newer, [Ollama](https://ollama.com), and a Gmail account made just for the support inbox (with 2-Step Verification and an [app password](https://myaccount.google.com/apppasswords)).

```bash
# 1. install and start the services
python -m venv .venv
.venv\Scripts\activate            # Windows (on macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
docker compose up -d

# 2. pull the local models
ollama pull qwen2.5:3b
ollama pull nomic-embed-text

# 3. create the Kafka topics (once)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic tickets --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic tickets.triaged --bootstrap-server localhost:9092

# 4. load past tickets into the vector database (once)
python rag/ingest.py

# 5. settings
copy .env.example .env            # then fill in SUPPORT_EMAIL, SUPPORT_APP_PASSWORD, AGENT_EMAIL
python gateway/inbound.py --check # tests the Gmail login, sending and Kafka
```

Run each of these in its own terminal:

```bash
python gateway/inbound.py                       # turns new emails into tickets
python classifier/consumer.py --max 0           # classifies and drafts replies
python dashboard/server.py                      # the console at http://localhost:8050
```

The first start of the console prints a username and password and saves them in `.env`.

**Phone access:** install [`cloudflared`](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/) and the ntfy app, then start the console with:

```bash
python dashboard/server.py --share
```

It prints a secure `https://...trycloudflare.com` link. The address changes on every start, and it works only while your computer and TriageIQ are running.

**Try it without email or an LLM:** `python dashboard/server.py --demo` shows the console with simulated tickets. Demo data is fake, and nothing is ever sent.


## Honest limits

- The past-ticket knowledge base is **synthetic** (hand-written examples), so draft quality on real company data is untested.
- Accuracy numbers are **not measured yet.** The accuracy charts only use generated test tickets that come with an answer key.
- The phone link depends on a laptop staying on. A production setup would deploy the services to a server.
- It uses a small 3B-parameter local model, so it is fast and private but less accurate than a large hosted model.


## Roadmap

- [x] Kafka in Docker, ticket producer
- [x] LLM classifier (category, priority, mood) with safe fallback
- [x] RAG over past tickets and drafted replies
- [x] Agent console with live updates and approval
- [x] Real email in and out, with approval before sending
- [x] Employee alerts by email and phone push
- [x] Sign-in, themes, mobile layout, phone access
- [ ] Accuracy evaluation on 100+ labelled tickets, with prompt tuning
- [ ] Langfuse monitoring of LLM latency and errors
- [ ] Larger, more realistic knowledge base
- [ ] CI with GitHub Actions
- [x] Console screenshots in this README
- [x] How-it-works deep dive in this README
- [ ] Measured results and a demo video in this README

Measured accuracy, latency and cost per ticket will be added here once the evaluation is done.

<p align="center">
  <img src="./assets/readme/footer.svg" width="100%" alt="The AI does the first 80%. A person decides what is sent. Built by Hemashree.">
</p>
