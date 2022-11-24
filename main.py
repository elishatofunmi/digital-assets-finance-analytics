from src.fetch_etl import fetchdata


if __name__ == "__main__":
    purl = "wss://mainnet.infura.io/ws/v3/3042024f3c65492fbef336a860926f9a"
    fd = fetchdata(provider_url = purl ,limit = 20000, batch_streaming=5)
    fd.fetch_batch()