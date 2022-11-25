from kafka_pipe import *
from decouple import config
import pandas as pd 
from time import sleep
import json


etl_kpro = kafka_etl_producer(config("etl_producer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_consumer"), config("etl_bootstrap_server"))

class MyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if hasattr(obj, '__str__'):  # This will handle ObjectIds
            return str(obj)

        return super(MyJsonEncoder, self).default(obj)



class kafka_pipe_etl:
    def __init__(self):
        return 

    def kafka_stream_batches(self, data1, data2):
        for data_one, data_two in zip (pd.read_csv(data1).iterrows(), pd.read_csv(data2).iterrows()):
            send_data = {
                "block": json.loads(json.dumps(data_one, indent=4, cls=MyJsonEncoder)),
                "transaction": json.loads(json.dumps(data_two, indent=4, cls=MyJsonEncoder))
            }
            etl_kpro.produce(send_data)
            print("sending data: ", send_data)
            sleep(5)
        return 

    def flush_data(self, data_path):
        print("Flushing data from memory....")
        os.remove(data_path)
        print("flushed "+ str(data_path) + " from memory!!!")
        return 



if __name__ == "__main__":
    transaction_dir = "exports/bf1d828a-212f-405f-837f-ae90032fac28_transactions.csv"
    block_dir = "exports/d135d272-f322-4637-abed-0210c3ab7feb_blocks.csv"

    kpe = kafka_pipe_etl()
    kpe.kafka_stream_batches(transaction_dir, block_dir)

