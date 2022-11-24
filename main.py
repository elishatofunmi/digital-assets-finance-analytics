from src.fetch_etl import fetchdata


if __name__ == "__main__":
    purl = "https://mainnet.infura.io/ws/v3/" + config("api_key") 
    fd = fetchdata(provider_url = purl ,limit = 20000, batch_streaming=5)
    fd.fetch_batch()

# curl -X GET "https://api.openweathermap.org/data/2.5/weather?zip=95050&appid=APIKEY&units=imperial"


# curl -X POST "https://mainnet.infura.io/v3/3042024f3c65492fbef336a860926f9a" -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# curl https://mainnet.infura.io/v3/3042024f3c65492fbef336a860926f9a \
#     -X POST \
#     -H "Content-Type: application/json" \
#     -d '{"jsonrpc":"2.0","method":"eth_accounts","params":[],"id":1}'

# curl --user :1fefc53f56574b9ebca02bb67f354aca \
#   https://mainnet.infura.io/v3/3042024f3c65492fbef336a860926f9a \
#   -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'