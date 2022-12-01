from kafka_pipe import *
from decouple import config
from web3 import Web3
from web3.middleware import geth_poa_middleware


infura_url = "https://mainnet.infura.io/v3/" + config("project_id") 
web3 = Web3(Web3.HTTPProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)


etl_kpro = kafka_etl_producer(config("etl_consumer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_producer"), config("etl_bootstrap_server"))


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
    max_transaction = []
    for bdata in data:
        transaction, block, token = clean_data(bdata)
        print("transaction: ", transaction)
        max_transaction.append(int(token[2]))
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
    print("maximum token value per block: ", max(max_transaction))
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


