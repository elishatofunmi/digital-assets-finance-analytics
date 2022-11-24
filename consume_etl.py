from json import loads
from kafka import KafkaConsumer
from kafka import TopicPartition

consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'], 
     auto_offset_reset='latest',
     enable_auto_commit=True,
     metadata_max_age_ms=5000)
     #group_id='rectvision01')
consumer.assign([TopicPartition('blocks', 0)])

for message in consumer:
 print(f"{message.value}")
