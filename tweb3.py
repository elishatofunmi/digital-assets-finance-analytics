from web3 import Web3
from web3.middleware import geth_poa_middleware


infura_url = "https://mainnet.infura.io/ws/v3/" + config("project_id") 

web3 = Web3(Web3.HTTPProvider(infura_url))

web3.middleware_stack.inject(geth_poa_middleware, layer=0)

# test_address = 

# print("isConnected: ", web.isConnected())

# balance = web3.eth.getBalance(test_address)
# print("balance:", we3.fromWei(balance, "ether"))

latest_block = web3.eth.getBlock("latest")
print("latest_block: ", latest_block)