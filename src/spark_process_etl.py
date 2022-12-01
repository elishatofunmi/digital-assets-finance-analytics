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

infura_url = "https://mainnet.infura.io/v3/" + config("project_id") 
web3 = Web3(Web3.HTTPProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

def compute_balance_diff(fromaddress, toaddress):
    fromaddress = Web3.toChecksumAddress(fromaddress)
    toaddress= Web3.toChecksumAddress(toaddress)
    frombalance = web3.eth.getBalance(fromaddress)
    tobalance = web3.eth.getBalance(toaddress)

    return frombalance - tobalance


def clean_data(data):
    transaction = [x.split(' ')[-1] for x in data['transaction'][-1].split('\n')]
    block = [x.split(' ')[-1] for x in data['block'][-1].split('\n')]
    token = [x.split(' ')[-1] for x in data['tokens'][-1].split('\n')]
    return transaction, block, token



def compute(data, number_of_blocks = 5):
    x, y, z = 0, 0, 0
    average_balance = 0
    for bdata in data:
        transaction, block, token = clean_data(bdata)
        x+= int(transaction[-2])
        y+= int(transaction[-3])
        z+= int(token[3])
        try:
            average_balance += compute_balance_diff(token[0], token[1])
        except Exception as err:
            average_balance += 0

    print("========================================================================")
    print("moving average, number of transactions, for a period of 5 blocks: ", x/number_of_blocks)
    print("Total value of gas/hour: ", y)
    print("Running count of number of transfers sent and received by addresses: ", z)
    print("Average balance over 5 blocks: ", average_balance/5)
    return 


#Spark context details
sc = SparkContext(appName="ardu-spark-consume-etl")
ssc = StreamingContext(sc,2)
#Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, [config("etl_consumer")], {"metadata.broker.list":config("etl_bootstrap_server")})
counts = dks.pprint().foreachRDD(compute)
#Starting Spark context
ssc.start()
ssc.awaitTermination()