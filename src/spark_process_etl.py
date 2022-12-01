#Imports and running findspark
import findspark
findspark.init('/etc/spark')
import pyspark
from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from decouple import config 


#Spark context details
sc = SparkContext(appName="ardu-spark-consume-etl")
ssc = StreamingContext(sc,2)
#Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, [config("etl_consumer")], {"metadata.broker.list":config("etl_bootstrap_server")})
counts = dks.pprint()
#Starting Spark context
ssc.start()
ssc.awaitTermination()