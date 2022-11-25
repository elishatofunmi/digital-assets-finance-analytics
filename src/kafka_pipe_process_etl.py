from kafka_pipe import *


etl_kpro = kafka_etl_producer(config("etl_consumer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_producer"), config("etl_bootstrap_server"))




if __name__ == "__main__":
    for message in etl_kcon.consumer:
        print(message.value)

