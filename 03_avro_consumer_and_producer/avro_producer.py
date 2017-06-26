import io
import random

import avro.schema
from kafka import SimpleProducer, KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVER

# To send messages synchronously
kafka = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                      retries=5)
producer = SimpleProducer(kafka)

# Kafka topic
topic = "my-topic"

# Path to .avsc avro schema file
schema = avro.schema.parse(open("SCHEMA.avsc").read())

for i in xrange(10):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"name": "123", "favorite_color": "111", "favorite_number": random.randint(0, 10)}, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send_messages(topic, raw_bytes)
