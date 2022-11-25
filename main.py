from decouple import config
from src.ethereum_fetch_etl import fetchdata


if __name__ == "__main__":
    purl = "https://mainnet.infura.io/v3/" + config("project_id") 
    fd = fetchdata(provider_url = purl ,limit = 20000, batch_streaming=5)
    fd.fetch_batch()

