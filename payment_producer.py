from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

# 
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producing messages to 'payments.raw'. Press Ctrl+C to exit.")

while True:
    event = {
        "transaction_id": str(uuid.uuid4()),  
        "ts_event": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "card_hash": str(uuid.uuid4())[:8],
        "merchant_id": "m" + str(random.randint(1, 10)),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "currency": random.choice(["USD", "EUR", "CAD"]),
        "mcc": random.randint(1000, 9999),
        "channel": random.choice(["online", "offline"]),
        "auth_result": random.choice(["approved", "declined"]),
        "location": random.choice(["NY", "LA", "TX", "FL"])
    }

   
    producer.send('payments.raw', event)
    producer.flush()

    print(f"Sent: {event}")
    time.sleep(2)
