import pandas
import csv
import random
import numpy as np
import time
from faker import Faker
from uuid import uuid4

start_time = time.time()
fake = Faker()


def get_transaction_amount():
    return round(random.randint(1, 1000) * random.random(), 2)


def get_transaction_date(fake):
    return fake.date_time_between(start_date="-1m", end_date="now").isoformat()


def create_financials_record():
    return {
        "transaction_id": "TID-" + str(uuid4()),
        "product_id": random.choice(products.product_id),
        "transaction_amount": get_transaction_amount(),
        "transaction_date": get_transaction_date(fake),
    }


def pareto_seed_picker(customer_no, transaction_no, shape):
    print(shape)
    seeds_list = []
    head_no = int(customer_no * 0.2)
    print(head_no)
    for seed in range(4500, 4510):
        np.random.seed(seed)
        sample = []
        custs = customers.customer_id
        pareto_shape = shape
        prob = np.random.pareto(pareto_shape, len(custs)) + 2
        prob /= np.sum(prob)
        for c in range(1, transaction_no + 1):
            sample.append(np.random.choice(custs, p=prob))
        prob_sum = (
            pandas.DataFrame(
                pandas.DataFrame(sample)[0].value_counts() /
                transaction_no *
                100) .head(head_no) .sum())
        seeds_list.append(((float(prob_sum)), seed))
        break
    print(min(seeds_list))
    return min(seeds_list)


def generator_by_picked_seed(seed, customer_no, transaction_no, shape):
    # TRANSACTION GENERATOR BY PICKED SEED
    head_no = int(customer_no * 0.2)
    np.random.seed(seed)
    sample = []
    custs = customers.customer_id
    pareto_shape = shape
    prob = np.random.pareto(pareto_shape, len(custs))
    prob /= np.sum(prob)
    for c in range(1, transaction_no + 1):
        sample.append(np.random.choice(custs, p=prob))
    prob_sum = (
        pandas.DataFrame(
            pandas.DataFrame(sample)[0].value_counts() / transaction_no * 100
        )
        .head(head_no)
        .sum()
    )
    print(prob_sum)
    transactions["customer_id"] = pandas.DataFrame(sample)
    return transactions


customer_no = 10000
transaction_no = 50000
products_no = 2500

product_id_df = pandas.read_csv(
    "../data/products_ids_no.csv", header=None, names=["product_id"]
)
customer_id_df = pandas.read_csv("../data/customers_ids_10000.csv")
products = product_id_df.head(products_no)
customers = customer_id_df.head(customer_no)

# seed = pareto_seed_picker(customer_no, transaction_no, 1)[1]
# print(seed)
print("Loaded data")
print("--- %s seconds ---" % (time.time() - start_time))

transactions = pandas.DataFrame(
    [create_financials_record() for _ in range(transaction_no)]
)
print("Generated transactions without customers")
transactions_generated = generator_by_picked_seed(
    4501, customer_no, transaction_no, 1.6
)
transactions_generated.to_csv(
    "../data/transactions_" + str(transaction_no) + ".csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
)

print("--- %s seconds ---" % (time.time() - start_time))
