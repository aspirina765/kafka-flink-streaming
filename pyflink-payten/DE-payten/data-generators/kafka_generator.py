import time
from uuid import uuid4
from faker import Faker
from kafka import KafkaProducer
from json import dumps
import pandas
import random

# TRANSACTION GENERATOR
start_time = time.time()
faker = Faker()


def get_client_id():
    return str(uuid4())


def get_transaction_amount():
    return round(random.randint(1, 1000) * random.random(), 2)


def get_transaction_date(fake):
    return fake.date_time_between(
        start_date="-60s",
        end_date="now").isoformat()


def get_credit_card_provider(fake):
    return fake.credit_card_provider()


def get_credit_card_number(fake):
    return fake.credit_card_provider()


def get_security_number(fake):
    return fake.credit_card_security_code()


def get_company(fake):
    return fake.company()


def get_job(fake):
    return fake.job()


def create_financials_record(fake):
    return {
        "client_id": get_client_id(),
        "credit_card_number": get_credit_card_number(fake),
        "credit_card_provider": get_credit_card_provider(fake),
        "credit_card_security_number": get_security_number(fake),
        "company": get_company(fake),
        "job": get_job(fake),
        "transaction_amount": get_transaction_amount(),
        "transaction_date": get_transaction_date(fake).replace("T", " "),
    }


def create_transactions(fake):
    transactions = pandas.DataFrame(
        [create_financials_record(fake) for _ in range(2000)]
    )
    for row in transactions.itertuples():
        client_id = row.client_id
        credit_card_number = row.credit_card_number
        credit_card_provider = row.credit_card_provider
        credit_card_security_number = row.credit_card_security_number
        company = row.company
        job = row.job
        transaction_amount = str(row.transaction_amount)
        transaction_date = row.transaction_date
        data = {
            "client_id": client_id,
            "credit_card_number": credit_card_number,
            "credit_card_provider": credit_card_provider,
            "credit_card_security_number": credit_card_security_number,
            "company": company,
            "job": job,
            "transaction_amount": transaction_amount,
            "transaction_date": transaction_date,
        }
        future = producer.send("transactions", value=data)
        result = future.get(timeout=60)
    print("--- %s seconds ---" % (time.time() - start_time))


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)
while True:
    create_transactions(faker)
    time.sleep(30)
