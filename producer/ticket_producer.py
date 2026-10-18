"""
TriageIQ - Step 2: fake ticket producer.

Sends realistic fake support tickets into the Kafka topic "tickets",
one after another, like a live stream of customers.

Run:
    python producer/ticket_producer.py                 # 20 tickets, 1 per second
    python producer/ticket_producer.py --count 200 --delay 0.2
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "tickets"

# category -> list of (message template, true priority, customer mood)
# The true priority and mood are saved as a hidden "label" so that in a later
# step we can measure how accurate the LLM classifier is.
TEMPLATES = {
    "Billing": [
        ("I was charged twice for my {product} subscription and I need a refund today!!", "High", "Angry"),
        ("Why is my invoice for {month} higher than usual? Please explain the charges.", "Medium", "Neutral"),
        ("Could you send me a GST invoice for my last payment?", "Low", "Neutral"),
        ("My payment failed but the money was deducted from my bank. Fix this now.", "High", "Angry"),
    ],
    "Technical": [
        ("The app crashes every time I open {feature}. I cannot work at all.", "High", "Angry"),
        ("Login keeps failing with error {code}. I have tried three times.", "High", "Frustrated"),
        ("The {feature} page is loading very slowly since yesterday.", "Medium", "Neutral"),
        ("How do I change the theme in the dashboard? Just curious.", "Low", "Happy"),
    ],
    "Delivery": [
        ("My order {order} has not arrived and it has been {days} days. This is unacceptable.", "High", "Angry"),
        ("Can I change the delivery address for order {order}?", "Medium", "Neutral"),
        ("Where can I track my package {order}?", "Low", "Neutral"),
        ("I received the wrong item in order {order}. Please arrange a replacement.", "Medium", "Frustrated"),
    ],
    "Account": [
        ("Someone logged in to my account from another country! Please secure it immediately.", "High", "Frustrated"),
        ("I cannot reset my password, the email never arrives.", "Medium", "Frustrated"),
        ("How do I update my phone number on my profile?", "Low", "Neutral"),
        ("Please delete my account and all my data.", "Medium", "Neutral"),
    ],
    "General": [
        ("Do you offer a student discount on the {product} plan?", "Low", "Neutral"),
        ("Just wanted to say thank you, the support team was really helpful!", "Low", "Happy"),
        ("Is there a mobile app for {feature}?", "Low", "Neutral"),
    ],
}

PRODUCTS = ["Pro", "Premium", "Starter", "Family", "Business"]
FEATURES = ["Reports", "Dashboard", "Checkout", "Settings", "Search"]
MONTHS = ["January", "February", "March", "April", "May", "June"]
CHANNELS = ["email", "chat", "app"]


def make_ticket(number: int) -> dict:
    """Build one fake ticket as a dictionary."""
    category = random.choice(list(TEMPLATES))
    template, priority, mood = random.choice(TEMPLATES[category])
    message = template.format(
        product=random.choice(PRODUCTS),
        feature=random.choice(FEATURES),
        month=random.choice(MONTHS),
        code=random.choice(["401", "500", "E-1042", "AUTH-7"]),
        order=f"ORD-{random.randint(10000, 99999)}",
        days=random.randint(3, 12),
    )
    return {
        "ticket_id": f"TKT-{10000 + number}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "channel": random.choice(CHANNELS),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "message": message,
        # hidden answer key, used later to measure the classifier's accuracy
        "label": {"category": category, "priority": priority, "mood": mood},
    }


stats = {"delivered": 0, "failed": 0}


def on_delivery(error, msg):
    """Called by Kafka after each send: tells us if it REALLY arrived."""
    if error is not None:
        stats["failed"] += 1
        key = msg.key().decode() if msg.key() else "?"
        print(f"  FAILED to deliver {key}: {error}")
    else:
        stats["delivered"] += 1


def main():
    parser = argparse.ArgumentParser(description="Send fake support tickets to Kafka")
    parser.add_argument("--count", type=int, default=20, help="how many tickets to send")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between tickets")
    args = parser.parse_args()

    # message.timeout.ms: give up on a ticket after 10 seconds instead of retrying forever
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "message.timeout.ms": 10000})

    # Check Kafka is really reachable BEFORE sending anything
    try:
        producer.list_topics(timeout=5)
    except KafkaException:
        print(f"Cannot reach Kafka at {BOOTSTRAP_SERVERS}.")
        print("Is it running? Try:  docker compose up -d   then   docker ps")
        sys.exit(1)

    print(f"Sending {args.count} tickets to topic '{TOPIC}' ... (Ctrl+C to stop)")

    try:
        for i in range(1, args.count + 1):
            ticket = make_ticket(i)
            producer.produce(
                TOPIC,
                key=ticket["ticket_id"],
                value=json.dumps(ticket),
                callback=on_delivery,
            )
            producer.poll(0)  # lets Kafka run the delivery callback
            label = ticket["label"]
            print(f"Queued {ticket['ticket_id']} [{label['category']}/{label['priority']}] {ticket['message']}")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush(15)  # wait for Kafka to confirm every ticket
        if stats["failed"] == 0 and stats["delivered"] > 0:
            print(f"Done. Delivered {stats['delivered']} tickets to Kafka.")
        else:
            print(f"WARNING: {stats['delivered']} delivered, {stats['failed']} failed. Check that Kafka is running.")


if __name__ == "__main__":
    main()