"""
TriageIQ - Step 3b: Kafka consumer that classifies live tickets.

Flow:  Kafka topic "tickets"  ->  LLM classifier  ->  Kafka topic "tickets.triaged"

For every ticket it prints the AI's answer next to the hidden answer key
(the "label" the producer attached), so you can see how accurate the AI is.
It also measures how long the AI took (latency) for the live dashboard.

Run:
    python classifier/consumer.py              # classify 10 tickets then stop
    python classifier/consumer.py --max 50     # classify 50 tickets
"""

import argparse
import json
import time

from confluent_kafka import Consumer, Producer

from classifier import classify_ticket

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "tickets"
OUTPUT_TOPIC = "tickets.triaged"
GROUP_ID = "triageiq-classifier"


def main():
    parser = argparse.ArgumentParser(description="Classify tickets from Kafka")
    parser.add_argument("--max", type=int, default=10, help="stop after this many tickets")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,            # Kafka remembers how far this group has read
        "auto.offset.reset": "earliest",  # first time: start from the oldest ticket
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    print(f"Waiting for tickets on '{INPUT_TOPIC}' (will classify {args.max}) ... Ctrl+C to stop\n")

    done = category_ok = priority_ok = 0
    try:
        while done < args.max:
            msg = consumer.poll(1.0)  # wait up to 1 second for a ticket
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                ticket = json.loads(msg.value())
                text = ticket["message"]
            except (json.JSONDecodeError, KeyError, TypeError):
                print("Skipped a message that is not a valid ticket")
                continue

            start = time.time()
            triage = classify_ticket(text)
            triage["latency_s"] = round(time.time() - start, 2)  # NEW: how long the AI took

            truth = ticket.get("label", {})
            cat_match = triage["category"] == truth.get("category")
            pri_match = triage["priority"] == truth.get("priority")

            done += 1
            category_ok += cat_match
            priority_ok += pri_match

            print(
                f"{ticket.get('ticket_id')} | AI: {triage['category']}/{triage['priority']}/{triage['mood']}"
                f" | truth: {truth.get('category')}/{truth.get('priority')}/{truth.get('mood')}"
                f" | category {'OK' if cat_match else 'WRONG'}, priority {'OK' if pri_match else 'WRONG'}"
                f" | {triage['latency_s']}s"
                + (" | NEEDS HUMAN" if triage.get("needs_human") else "")
            )

            ticket["triage"] = triage
            producer.produce(OUTPUT_TOPIC, key=str(ticket.get("ticket_id")), value=json.dumps(ticket))
            producer.poll(0)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        consumer.close()
        if done:
            print(f"\nClassified {done} tickets")
            print(f"Category accuracy: {category_ok}/{done} ({100 * category_ok / done:.0f}%)")
            print(f"Priority accuracy: {priority_ok}/{done} ({100 * priority_ok / done:.0f}%)")


if __name__ == "__main__":
    main()