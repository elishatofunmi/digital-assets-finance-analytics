from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads, dumps
from kafka import TopicPartition
import time

class kafka_etl_consumer:
    def __init__(self, topic = 'test-topic', bootstrap_servers = '0.0.0.0:9092'):
        """This function is used to create a kafka consumer."""
        self.consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers,
                                    auto_offset_reset = 'latest',
                                    enable_auto_commit = False,
                                    #group_id = self.data_converter_consumer_group_id,
                                    value_deserializer = lambda x: loads(x.decode('utf-8'))
                                )
        self.consumer.assign([TopicPartition(topic, 0)])

        return

class kafka_etl_producer:
    def __init__(self, topic = 'test_topic', bootstrap_servers= '0.0.0.0:9092'):

        self.producer = KafkaProducer(bootstrap_servers=[bootstrap_servers], 
        value_serializer=lambda x: dumps(x).encode('utf-8'))
        self.topic = topic

        return
    def produce(self, data):
        self.producer.send(self.topic, data)
        return 
