import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError

from config import KAFKA_BOOTSTRAP_SERVER


class BasicKafkaProducer(object):
    def __init__(self):
        try:
            print ("Initialising Kafka Producer")
            self.producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                                          retries=5,
                                          max_block_ms=10000,
                                          value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8'))
        except NoBrokersAvailable:
            print (u'Kafka Host not available: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            self.producer = None

    def send_message(self, topic_name, message, key=None):
        """
        :param topic_name: topic name
        :param key: key to decide partition
        :param message: json serializable object to send
        :return:
        """
        if not self.producer:
            print(u'Kafka Host not available: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            return
        try:
            start = time.time()
            self.producer.send(topic_name, key=key, value=message)
            print(u'Time take to push to Kafka: {}'.format(time.time() - start))
        except KafkaTimeoutError as e:
            print (u'Message not sent: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            print(e)
            pass
        except Exception as e:
            print(u'Message not sent: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            print (e)
            pass

    def close(self):
        try:
            self.producer.close()

        except KafkaError as e:
            print("Error closing connection to Kafka")
            print ("Error closing connection to Kafka")
            print (e)


if __name__ == "__main__":
    _producer = BasicKafkaProducer()
    _producer.send_message(topic_name='test-topic-example',
                           message='{"test_col": "column_value"}'
                           )
    _producer.close()
