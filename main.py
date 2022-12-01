from decouple import config
from src.ethereum_fetch_etl import fetchdata


if __name__ == "__main__":
    purl = "https://mainnet.infura.io/v3/" + config("project_id") 
    fd = fetchdata(provider_url = purl ,limit = 20000, batch_streaming=5)
    fd.fetch_batch()



a = ['1449662735', '664728', '0xcd7211d81b6e675dc1735d0b4964fcce214c3c4047b6...', '0x604a066a3f023658a991e1096cdd74df6df8244dba31...', '0x45ac6e8f0a55f0c7', '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a...', '0x00000000000000000000000000000000000000000000...', '0xae149a2e9076b7f93a1b52d66db0ee1e2d7d160bb564...', '0xc41ce7b19776712f60e54ed2b1ee2907c3ae0ad6244b...', '0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5', '7471075092698', '3981635753920326340', '770', '0xd783010203844765746887676f312e342e32856c696e...', '3141592', '42000', '2', 'object']
