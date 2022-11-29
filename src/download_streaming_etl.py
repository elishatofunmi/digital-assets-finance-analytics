from json import loads
from kafka import KafkaConsumer
from kafka import TopicPartition
from decouple import config
import uuid, os, sys
import pandas as pd 


class consume_etl:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            bootstrap_servers=[config("etl_bootstrap_server")], 
            auto_offset_reset='latest',
            enable_auto_commit=True,
            metadata_max_age_ms=5000)
            #group_id='rectvision01')
        self.consumer.assign([TopicPartition(topic, 0)])


class transaction_etl:
    def __init__(self):
        self.data = {"transaction_index": [],"from_address": [], "hash": [], "value": [], "nonce":[],
        "to_address":[], "gas": [], "gas_price": [], "input": [],
        "receipt_cumulative_gas_used": [], "receipt_gas_used": [], "receipt_contract_address": [],"receipt_root": [],
        "receipt_status": [], "block_timestamp":[], "block_number": [], "block_hash": [], "max_fee_per_gas": [],
        "max_priority_fee_per_gas": [],"transaction_type": [], "receipt_effective_gas_price": []}

        return 

    def process(self, etl_data):
        '''
        ====================
        schema defination
        ====================
        '''
        self.data['transaction_index'].append(etl_data['transaction_index'])
        self.data['from_address'].append((etl_data['from_address']))
        self.data['hash'].append(etl_data['hash'])
        self.data['value'].append(etl_data['value'])
        self.data['nonce'].append(etl_data['nonce'])
        self.data['to_address'].append(etl_data['to_address'])
        self.data['gas'].append(etl_data['gas'])
        self.data['gas_price'].append(etl_data['gas_price'])
        self.data['input'].append(etl_data['input'])
        self.data['receipt_cumulative_gas_used'].append(etl_data['receipt_cumulative_gas_used'])
        self.data['receipt_gas_used'].append(etl_data['receipt_gas_used'])
        self.data['receipt_contract_address'].append(etl_data['receipt_contract_address'])
        self.data['receipt_root'].append(etl_data['receipt_root'])
        self.data['receipt_status'].append(etl_data['receipt_status'])
        self.data['block_timestamp'].append(etl_data['block_timestamp'])
        self.data['block_number'].append(etl_data['block_number'])
        self.data['block_hash'].append(etl_data['block_hash'])
        self.data['max_fee_per_gas'].append(etl_data['max_fee_per_gas'])
        self.data['max_priority_fee_per_gas'].append(etl_data['max_priority_fee_per_gas'])
        self.data['transaction_type'].append(etl_data['transaction_type'])
        self.data['receipt_effective_gas_price'].append(etl_data['receipt_effective_gas_price'])
        return 


class block_etl:
    def __init__(self):
        self.data = {"timestamp": [],"number": [], "hash": [], "parent_hash": [], "nonce":[],
        "sha3_uncles":[], "logs_bloom": [], "state_root": [],
        "receipts_root": [], "miner": [], "difficulty": [],"total_difficulty": [],
        "size": [], "extra_data":[], "gas_limit": [], "gas_used": [], "transaction_count": []}

        return 

    def process(self, etl_data):
        '''
        ====================
        schema defination
        ====================
        '''
        self.data['timestamp'].append(etl_data['timestamp'])
        self.data['number'].append(int(etl_data['number']))
        self.data['hash'].append(etl_data['hash'])
        self.data['parent_hash'].append(etl_data['parent_hash'])
        self.data['nonce'].append(etl_data['nonce'])
        self.data['sha3_uncles'].append(etl_data['sha3_uncles'])
        self.data['logs_bloom'].append(etl_data['logs_bloom'])
        # self.data['transactions_root'].append(etl_data['transaction_root'])
        self.data['state_root'].append(etl_data['state_root'])
        self.data['receipts_root'].append(etl_data['receipts_root'])
        self.data['miner'].append(etl_data['miner'])
        self.data['difficulty'].append(etl_data['difficulty'])
        self.data['total_difficulty'].append(etl_data['total_difficulty'])
        self.data['size'].append(etl_data['size'])
        self.data['extra_data'].append(etl_data['extra_data'])
        self.data['gas_limit'].append(etl_data['gas_limit'])
        self.data['gas_used'].append(etl_data['gas_used'])
        self.data['transaction_count'].append(int(etl_data['transaction_count']))

        return 

class token_etl:
    def __init__(self):
        self.data = {"token_address": [],"from_address": [], "to_address": [], "value": [], "transaction_hash":[],
        "log_index":[], "block_timestamp": [], "block_number": [],
        "block_hash": []}

        return 

    def process(self, etl_data):
        '''
        ====================
        schema defination
        ====================
        '''

        # self.data['token_address'].append(etl_data['token_address'])
        self.data['from_address'].append(etl_data['from_address'])
        self.data['to_address'].append(etl_data['to_address'])
        self.data['value'].append(int(etl_data['value']))
        #self.data['transaction_hash'].append(etl_data['transaction_hash'])
        self.data['log_index'].append(etl_data['log_index'])
        self.data['block_timestamp'].append(etl_data['block_timestamp'])
        self.data['block_number'].append(int(etl_data['block_number']))
        self.data['block_hash'].append(etl_data['block_hash'])
       
        return 
# define block etl by schema
betl = block_etl()

# define transaction etl by schema
tetl = transaction_etl()

# define tokens etl by schema
tokenetl = token_etl()

# consume blocks and transaction from kafka stram
consume_etl_blocks = consume_etl(topic= config("consumer_etl_blocks"))
consume_etl_transactions = consume_etl(topic=config("consumer_etl_transactions"))
consume_etl_tokens = consume_etl(topic=config("consumer_etl_tokens"))

# initialize output csvs
blocks_dir = str(uuid.uuid4()) + "export_blocks.csv"
transactions_dir = str(uuid.uuid4()) + "export_transactions.csv"
tokens_dir = str(uuid.uuid4()) + "export_tokens.csv"




if __name__ == "__main__":
    print("running....")
    for i, (blocks, transactions, tokens) in enumerate(zip(consume_etl_blocks.consumer, consume_etl_transactions.consumer, consume_etl_tokens.consumer)):
        print(i)
        if i < 20:
            b_value = loads(blocks.value.decode('utf-8'))
            t_value = loads(transactions.value.decode('utf-8'))
            tok_value = loads(transactions.value.decode('utf-8'))
            betl.process(b_value)
            tetl.process(t_value)
            tokenetl.process(tok_value)
        else:
            break

        if i%10==0:
            print("current_count: ", i)

    print("completed....")

    print("converting to dataframe...")
    betl_dataframe = pd.DataFrame(betl.data)
    tetl_dataframe = pd.DataFrame(tetl.data)
    toketl_dataframe= pd.DataFrame(tokenetl.data)


    print("exporting to csv...")
    betl_dataframe.to_csv(blocks_dir, index=False)
    tetl_dataframe.to_csv(transactions_dir, index=False)
    tokenetl_dataframe.to_csv(tokens_dir, index=False)


    print("export locations: ")
    print("blocks: ", blocks_dir)
    print("transactions: ", transactions_dir)
    print("tokens: ", tokens_dir)

