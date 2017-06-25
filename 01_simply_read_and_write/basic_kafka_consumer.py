import json

from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP_SERVER


class BasicKafkaConsumer(object):
    def __init__(self, topic_list):
        print ("Initialising Kafka Consumer")
        self.consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                                      client_id='SimpleKafkaConsumer',
                                      group_id='1',
                                      auto_offset_reset='earliest',
                                      value_deserializer=lambda m: json.loads(m, ensure_ascii=False).encode('utf-8')
                                      )
        self.consumer.subscribe(topic_list)


if __name__ == "__main__":
    _consumer = BasicKafkaConsumer(topic_list=['test-topic-example']).consumer
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
