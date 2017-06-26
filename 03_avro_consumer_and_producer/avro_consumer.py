import io

import avro.io
from kafka import KafkaConsumer

SCHEMA = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}

# To consume messages
consumer = KafkaConsumer('my-topic',
                         group_id='my_group',
                         bootstrap_servers=['localhost:9092'])

schema = avro.schema.parse(open("SCHEMA.avsc").read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)
    print user1
