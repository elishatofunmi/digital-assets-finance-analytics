import pandas as pd 
import uuid, os, sys
from decouple import config
from .kafka_etl import kafka_etl_producer, kafka_etl_consumer

etl_kpro = kafka_etl_producer(config("etl_producer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_consumer"), config("etl_bootstrap_server"))

class fetchdata:

    def __init__(self, limit=20000, batch_streaming = 5):
        # initialize fetch data params
        self.limit = limit
        self.batch_streaming= batch_streaming

        try:
            # create a transaction_output folder if folder does not exists
            if os.path.exists('/trasaction_output'):
                pass 
            else: 
                os.mkdir("trasaction_output")

            # create a block_output folder if folder does not exists

            if os.path.exists('/block_output'):
                pass 
            else:
                os.mkdir("block_output")
        except FileExistsError as err:
            pass
        
        return 

    def fetch_batch(self):
        """
        fetch the latest block & transaction data from provider url, based on limit.
        """
        start_block = 0
        end_block = 50000
        block_output="/block_output/"+ str(uuid.uuid4()) + ".csv"
        trasaction_output = "/trasaction_output" + str(uuid.uuid4()) + ".csv"
        provider_url = "https://celo-mainnet.infura.io/v3/1b37de4d60cc470990fb94e885fde24c"
        input_command = (str(start_block), str(end_block), block_output, trasaction_output, provider_url)
        # execute job
        self.run_job(input_command)

        # evaluate data
        self.test_data_if_csv(block_output) # test block data
        self.test_data_if_csv(transaction_output) # test transaction data

        # batch stream via kafka
        self.kafka_stream_batches(block_output, transaction_output)

        # flush data
        self.flush_data(block_output)
        self.flush_data(transaction_output)
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

    def test_data_if_csv(self, data_path):
        print("=====================================")
        data = pd.read_csv(data_path)
        print(data.head())
        print("=====================================")
        return 


    def run_job(self, *input_command):
         # input_command = (start_block, end_block, block_output, trasaction_output, provider_url)
         print(input_command)
         type(input_command)
         command = "ethereumetl export_blocks_and_transactions --start-block %s --end-block %s --blocks-output %s --transactions-output %s --provider-uri %s" % input_command
         print('running command: ', command)
         os.system(command)
         return 