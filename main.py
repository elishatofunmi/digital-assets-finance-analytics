from src.fetch_etl import fetchdata


if __name__ == "__main__":
    fd = fetchdata(limit = 20000, batch_streaming=5)
    fd.fetch_batch()