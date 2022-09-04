import time
import pandas
import random

from faker import Faker
from kafka import KafkaProducer
from time import sleep
from json import dumps
from random import randint

start_time = time.time()

# TRANSACTION GENERATOR
fake = Faker()


def get_transaction_amount():
    return round(random.randint(1, 1000) * random.random(), 2)


def get_transaction_date(fake):
    return fake.date_time_between(
        start_date="-60s",
        end_date="now").isoformat()


def create_financials_record():
    return {
        "transaction_amount": get_transaction_amount(),
        "transaction_date": get_transaction_date(fake),
    }


def create_transactions():
    transactions = pandas.DataFrame(
        [create_financials_record() for _ in range(2000)]
    )
    for row in transactions.itertuples():
        data1 = str(row.transaction_amount)
        data2 = row.transaction_date + "Z"
        data = {"transaction_amount": data1, "transaction_date": data2}
        future = producer.send("transactions", value=data)
        result = future.get(timeout=60)
    print("--- %s seconds ---" % (time.time() - start_time))


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)
while True:
    create_transactions()
    time.sleep(30)
