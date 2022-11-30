from kafka_pipe import *
from decouple import config


etl_kpro = kafka_etl_producer(config("etl_consumer"), config("etl_bootstrap_server"))
etl_kcon = kafka_etl_consumer(config("etl_producer"), config("etl_bootstrap_server"))


def compute(data, number_of_blocks = 5):
    x, y, z = 0, None, None
    for bdata in data:
    
        print("test: ", bdata['transaction'])
        x+= float(bdata['transaction'][-1])
        y+= float(bdata['transaction'][-2])
        z+= float(bdata['tokens'][3])

    print("========================================================================")
    print("moving average, number of transactions, for a period of 5 blocks: ", x/number_of_blocks)
    print("Total value of gas/hour: ", y)
    print("Running count of number of transfers sent and received by addresses: ", z)
    print("========================================================================")
    return 


if __name__ == "__main__":
    count = 0
    batch_data = []
    for message in etl_kcon.consumer: # data per 5 blocks
        # print(message.value)
        if len(batch_data) == 5:
            compute(batch_data)

            # reset batch to blocks of 5
            batch_data = []
            batch_data.append(message.value)
        else:
            batch_data.append(message.value)

       



