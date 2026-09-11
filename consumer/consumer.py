import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "banking-analytics-group",
    "auto.offset.reset": "earliest"  #从topic里最早还存在的消息开始读

})

TOPIC = "banking-events"

consumer.subscribe([TOPIC])

print(f'Listening to topic {TOPIC}')

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f'Consumer Error: {msg.error()}')
            continue

        event = json.loads(msg.value().decode('utf-8'))

        print("Event received:")
        print(event)

finally:
    consumer.close()