from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import KAFKA_BOOTSTRAP_SERVER


class BasicKafkaConsumer(object):
    """
    This is a basic Kafka Consumer class which can read messages into kafka

    KAFKA_BOOTSTRAP_SERVER is the connection details to the Kafka broker
    it could be one or a list of brokers example: ['localhost:9092']
    """

    def __init__(self, topic_list):
        try:
            print ("Initialising Kafka Consumer")
            self.consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                                          client_id='SimpleKafkaConsumer',
                                          group_id='1',
                                          auto_offset_reset='earliest',
                                          )
            self.consumer.subscribe(topic_list)
        except NoBrokersAvailable:
            print (u'Kafka Host not available: {}'.format(KAFKA_BOOTSTRAP_SERVER))
            self.consumer = None


if __name__ == "__main__":
    _consumer = BasicKafkaConsumer(topic_list=['test-topic']).consumer
    for message in _consumer:
        print ("topic name: ", message.topic)
        print ("partition: ", message.partition)
        print ("offset: ", message.offset)
        print ("timestamp: ", message.timestamp)
        print ("timestamp type: ", message.timestamp_type)
        print ("key: ", message.key)
        print ("value: ", message.value)
        print ("checksum: ", message.checksum)
        print ("serialized key size: ", message.serialized_key_size)
        print ("serialized value size: ", message.serialized_value_size)
        print ("\n")
