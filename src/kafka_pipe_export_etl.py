from kafka_pipe import *
from decouple import config


etl_kpro = kafka_etl_producer(config("etl_producer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_consumer"), config("etl_bootstrap_server"))


class kafka_pipe_etl:
    def __init__(self):
        return 

    def kafka_stream_batches(self, data1, data2):
        for data_one, data_two in zip (pd.read_csv(data1), pd.read_csv(data2)):
            send_data = {
                "block": data_one,
                "transaction": data_two
            }
            etl_kpro.produce(send_data)
            print("sending data: ", send_data)
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
    kep.kafka_stream_batches(transaction_dir, block_dir)

