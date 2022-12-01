from kafka_pipe import *
from decouple import config
from web3 import Web3
from web3.middleware import geth_poa_middleware


infura_url = "https://mainnet.infura.io/v3/" + config("project_id") 
web3 = Web3(Web3.HTTPProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)


etl_kpro = kafka_etl_producer(config("etl_consumer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_producer"), config("etl_bootstrap_server"))

def estimate_erc_20(address):
    abi = [{"inputs":[{"internalType":"address[]","name":"addresses","type":"address[]"},
    {"internalType":"uint256[]","name":"balances","type":"uint256[]"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,
    "inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,
    "internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256",
    "name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,
    "internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},
    {"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[{"internalType":"address",
    "name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[
        {"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256",
        "name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],
        "stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],
        "name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],
        "name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],
        "name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],
        "name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},
        {"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],
        "stateMutability":"nonpayable","type":"function"}]

    address = web3.toChecksumAddress(address)
    contract = web3.eth.contract(address=address, abi=abi)

    # Let's print Name of Token
    return contract.functions.name()#.transact()
    

def estimate_gas(fromaddress, nonce, toaddress, data):
     value = web3.eth.estimateGas({
     "from"      : web3.toChecksumAddress(fromaddress),       
     "nonce"     : nonce, 
     "to"        : web3.toChecksumAddress(toaddress),     
     "data"      : data
     })
     return value

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

def headers(data):
    transaction = [''.join(x.split(' ')[:-1]) for x in data['transaction'][-1].split('\n')]
    block = [''.join(x.split(' ')[:-1]) for x in data['block'][-1].split('\n')]
    token = [''.join(x.split(' ')[:-1]) for x in data['tokens'][-1].split('\n')]
    return transaction, block, token



def compute(data, number_of_blocks = 5):
    x, y, z = 0, 0, 0
    average_balance = 0
    max_transaction = []
    total_gas_estimate = 0
    highest_transaction_in_block = []
    erc_smart_contract = []
    for bdata in data:
        
        transaction, block, token = clean_data(bdata)
        max_transaction.append(int(token[2]))
        x+= int(transaction[-2]) # transaction count
        y+= int(transaction[-3])
        z+= int(token[3])
        total_gas_estimate += estimate_gas(block[1],block[4], block[5], block[3])
        highest_transaction_in_block.append(int(transaction[-2]))
        erc_smart_contract.append(estimate_erc_20(token[0]))
        try:
            average_balance += compute_balance_diff(token[0], token[1])
        except Exception as err:
            average_balance += 0

        

    print("========================================================================")
    print("moving average, number of transactions, for a period of 5 blocks: ", x/number_of_blocks)
    print("average gas limit in block: ", y)
    print("highest transaction in a block: ", max(highest_transaction_in_block))
    print("average gas estimate /hour: ", total_gas_estimate/5)
    print("Running count of number of transfers sent and received by addresses: ", z)
    print("Average balance over 5 blocks: ", average_balance/5)
    print("maximum token value per block: ", max(max_transaction))
    print("number of smart contracts per block: ", erc_smart_contract)
    return 


if __name__ == "__main__":
    count = 0
    batch_data = []
    for message in etl_kcon.consumer: # data per 5 blocks
        # print(message.value)
        if len(batch_data) == 5:
            compute(batch_data)

            # reset batch to blocks of 5
            batch_data = []
            batch_data.append(message.value)
        else:
            batch_data.append(message.value)


