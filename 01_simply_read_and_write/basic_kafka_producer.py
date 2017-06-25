import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_BOOTSTRAP_SERVER


class BasicKafkaProducer(object):
    def __init__(self):
        try:
            print ("Initialising Kafka Producer")
            self.producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                                          retries=5,
                                          max_block_ms=10000,
                                          value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8'))
        except KafkaError as e:
            print("Error making connection to Kafka")
            print (e)

    def send(self, message):
        try:
            self.producer.send('test-topic-example', message)

        except KafkaError as e:
            print ("Error closing connection to Kafka")
            print (e)

    def close(self):
        try:
            self.producer.close()

        except KafkaError as e:
            print("Error closing connection to Kafka")
            print ("Error closing connection to Kafka")
            print (e)


if __name__ == "__main__":
    _producer = BasicKafkaProducer()
    _producer.send('{"test_col": "column_value"}')
    _producer.close()
