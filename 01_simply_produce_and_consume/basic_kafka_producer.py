import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError

from config import KAFKA_BOOTSTRAP_SERVER


class BasicKafkaProducer(object):
    """
    This is a basic Kafka Producer class which can push String messages into kafka

    KAFKA_BOOTSTRAP_SERVER is the connection details to the Kafka broker
    it could be one or a list of brokers example: ['localhost:9092']

    This producer tries 5 times to push the message into kafka in case of failure
    """

    def __init__(self):
        try:
            print ("Initialising Kafka Producer")
            self.producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                                          retries=5)
        except NoBrokersAvailable:
            print (u'Kafka Host not available: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            self.producer = None

    def send_message(self, topic_name, message, key=None):
        """
        :param topic_name: topic name
        :param key: key to decide partition
        :param message: String object to send
        :return:
        """
        if not self.producer:
            print(u'Kafka Host not available: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            return
        try:
            start = time.time()
            self.producer.send(topic_name, value=message)
            print(u'Time taken to push to Kafka: {}'.format(time.time() - start))
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
            if not self.producer:
                print(u'No active producer: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            else:
                self.producer.close()

        except KafkaError as e:
            print(u'Error closing connection to Kafka Host: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            print (e)

# if __name__ == "__main__":
#     _producer = BasicKafkaProducer()
#     _producer.send_message(topic_name='test-topic-example',
#                            message='{"test_col": "column_value"}'
#                            )
#     _producer.close()
