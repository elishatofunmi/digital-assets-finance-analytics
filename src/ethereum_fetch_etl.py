import pandas as pd 
import uuid, os, sys
from decouple import config


class fetchdata:

    def __init__(self, provider_url, limit=20000, batch_streaming = 5):
        # initialize fetch data params
        self.limit = limit
        self.batch_streaming= batch_streaming
        self.provider_url= provider_url

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
        end_block = 20000
        block_output="/block_output/"+ str(uuid.uuid4()) + ".csv"
        transaction_output = "/trasaction_output" + str(uuid.uuid4()) + ".csv"
        input_command = (start_block, end_block, block_output, transaction_output, self.provider_url)
        # execute job
        self.run_job(input_command)

        return 



    def run_job(self, input_command):
         # input_command = (start_block, end_block, block_output, trasaction_output, provider_url)
         print(input_command)
         type(input_command)
         command = "ethereumetl export_blocks_and_transactions --start-block %i --end-block %i --blocks-output %s --transactions-output %s --provider-uri %s" % (input_command)
         print('running command: ', command)
         os.system(command)
         return 