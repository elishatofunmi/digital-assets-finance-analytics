
from decouple import config
import os, sys

provider_url = "https://mainnet.infura.io/v3/" + config("project_id")
kafka_id = config("etl_bootstrap_server")
batch_size = int(config("batch_size"))
command = "ethereumetl stream --provider-uri %s -e block,transaction,token_transfer --output=kafka/%s --batch-size %i" % (provider_url, kafka_id, batch_size)


if __name__ == "__main__":
    print("running command: ", command)
    os.system(command)