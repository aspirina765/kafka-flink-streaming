import time
import csv
from faker import Faker
import pandas
import datetime
from random import randint
import random
from uuid import uuid4

fake = Faker()
start_time = time.time()

number = 10000


def first_name_and_gender():
    g = "M" if random.randint(0, 1) == 0 else "F"
    n = fake.first_name_male() if g == "M" else fake.first_name_female()
    return g, n


def title_office_org():
    # generate a map of real office to fake office
    offices = ["New York", "Austin", "Seattle", "Chicago"]
    # codify the hierarchical structure
    allowed_orgs_per_office = {
        "New York": ["Sales"],
        "Austin": ["Devops", "Platform", "Product", "Internal Tools"],
        "Chicago": ["Devops"],
        "Seattle": ["Internal Tools", "Product"],
    }
    allowed_titles_per_org = {
        "Devops": ["Engineer", "Senior Engineer", "Manager"],
        "Sales": ["Associate"],
        "Platform": ["Engineer"],
        "Product": ["Manager", "VP"],
        "Internal Tools": ["Engineer", "Senior Engineer", "VP", "Manager"],
    }

    office = random.choice(offices)
    org = random.choice(allowed_orgs_per_office[office])
    title = random.choice(allowed_titles_per_org[org])
    return {"office": office, "title": title, "org": org}


def salary_and_bonus():
    salary = round(random.randint(90000, 120000) / 1000) * 1000
    bonus_ratio = random.uniform(0.15, 0.2)
    bonus = round(salary * bonus_ratio / 500) * 500
    return {"salary": salary, "bonus": bonus}


def birth_and_start_date():
    sd = fake.date_between(start_date="-20y", end_date="now")
    delta = datetime.timedelta(days=365 * randint(18, 40))
    bd = sd - delta
    return bd, sd


def title_office_org_salary_bonus():
    position = title_office_org()
    title_and_salary_range = {
        "Engineer": [90, 120],
        "Senior Engineer": [110, 140],
        "Manager": [130, 150],
        "Associate": [60, 80],
        "VP": [150, 250],
    }
    salary_range = title_and_salary_range[position["title"]]

    salary = (
        round(
            random.randint(
                1000 *
                salary_range[0],
                1000 *
                salary_range[1]) /
            1000) *
        1000)
    bonus_ratio = random.uniform(0.15, 0.2)
    bonus = round(salary * bonus_ratio / 500) * 500
    position.update({"salary": salary, "bonus": bonus})
    return position


def create_customer_record():
    gender, first_name = first_name_and_gender()
    birth_date, start_date = birth_and_start_date()
    position = title_office_org_salary_bonus()
    return {
        "customer_id": "CID-" + str(uuid4()),
        "first_name": first_name,
        "last_name": fake.last_name(),
        "gender": gender,
        "ssn": fake.ssn(),
        "credit_card": fake.credit_card_number(),
        "credit_card_provider": fake.credit_card_provider(),
        "birth_date": birth_date,
        "start_date": start_date,
        "title": position["title"],
        "office": position["office"],
        "organization": position["org"],
        "salary": position["salary"],
        "bonus": position["bonus"],
        "accured_holidays": random.randint(0, 20),
    }


customers = pandas.DataFrame([create_customer_record() for _ in range(number)])
print("Created data")
customers.to_csv(
    "../data/customers_" + str(number) + ".csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
)
pandas.DataFrame(customers.customer_id).to_csv(
    "../data/customers_ids_no_" + str(number) + ".csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
)
print("Exported data")
print("--- %s seconds ---" % (time.time() - start_time))
