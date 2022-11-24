from json import loads
from kafka import KafkaConsumer
from kafka import TopicPartition
from decouple import config

consumer = KafkaConsumer(
     bootstrap_servers=[config("etl_bootstrap_server")], 
     auto_offset_reset='latest',
     enable_auto_commit=True,
     metadata_max_age_ms=5000)
     #group_id='rectvision01')
consumer.assign([TopicPartition(config("etl_topic"), 0)])

for message in consumer:
 print(f"{message.value}")
