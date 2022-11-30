from kafka_pipe import *
from decouple import config
import pandas as pd 
from time import sleep
import json, datetime


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

    def kafka_stream_batches(self, data1, data2, data3):
        for data_one, data_two, data_three in zip (pd.read_csv(data1).iterrows(), pd.read_csv(data2).iterrows(), pd.read_csv(data2).iterrows()):
            send_data = {
                "block": json.loads(json.dumps(data_one, indent=4, cls=MyJsonEncoder)),
                "transaction": json.loads(json.dumps(data_two, indent=4, cls=MyJsonEncoder)),
                "tokens": json.loads(json.dumps(data_three, indent=4, cls=MyJsonEncoder))
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
    blocks = "exports/86647c62-99b3-47ab-97da-000315df9618export_blocks.csv"
    transactions = "exports/73005056-fbe4-4786-bdd4-8138ee87eb3aexport_transactions.csv"
    tokens = "exports/bc43eed9-4d43-4ea7-838a-34c8043bf1e0export_tokens.csv"

    # steam csv data for processing
    kpe = kafka_pipe_etl()
    kpe.kafka_stream_batches(transactions, blocks, tokens)

    # Flush Csv from memory
    # kpe.flush_data(blocks)
    # kpe.flush_data(transactions)
    # kpe.flush_data(tokens)
