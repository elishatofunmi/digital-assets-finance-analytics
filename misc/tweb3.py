from decouple import config
from web3 import Web3
from web3.middleware import geth_poa_middleware


infura_url = "https://mainnet.infura.io/v3/" + config("project_id") 

web3 = Web3(Web3.HTTPProvider(infura_url))

web3.middleware_onion.inject(geth_poa_middleware, layer=0)

# test_address = 

# print("isConnected: ", web.isConnected())

# balance = web3.eth.getBalance(test_address)
# print("balance:", we3.fromWei(balance, "ether"))

latest_block = web3.eth.getBlock("latest")
print("latest_block 1st: ", latest_block[0])
print("latest_block 2nd: ", latest_block[1])

# ethereumetl export_blocks_and_transactions --start-block 0 --end-block 500000 \
# --provider-uri https://mainnet.infura.io/v3/1b37de4d60cc470990fb94e885fde24c \
# --blocks-output blocks.csv --transactions-output transactions.csv


"ethereumetl stream --provider-uri https://mainnet.infura.io/v3/1b37de4d60cc470990fb94e885fde24c -e block,transaction,token_transfer --output=kafka_web3/localhost:9092 --batch-size 10"