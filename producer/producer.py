import json
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

#1.kafka connection
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
})

#2. Topic
TOPIC = "banking-events"

#3. Create one banking event

event={
    'event_id':str(uuid4()),
    "customer_id":"C_10001",
    "event_type":"onboarding_started",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "channel": "mobile_app",
    "device_type": "ios",
    "session_id": "S_12345",
    "properties":{
        "campaign_id": "CMP_001"
    }
}

#4+5 serialize and send
producer.produce(
    TOPIC,
    value=json.dumps(event).encode("utf-8")
    )

producer.flush()

print("Event sent")
print(event)

# 新建 real-time-digital-banking-analytics repo
# 建上述最小目录
# requirements.txt 加 confluent-kafka
# 创建 compose.yaml，只起 一个 Kafka broker
# 验证 Kafka 正常运行
# 自己写第一版 producer.py
# 发一条 onboarding_started
# 再写 consumer.py
# consumer 成功打印这条 JSON