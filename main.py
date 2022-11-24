from src.fetch_etl import fetchdata

fd = fetchdata(limit = 20000, batch_streaming=5)
