import os
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import (
    TableConfig,
    DataTypes,
    BatchTableEnvironment,
    EnvironmentSettings,
    StreamTableEnvironment,
    TableEnvironment,
)
from pyflink.table.descriptors import Schema, OldCsv, Csv, FileSystem

environment_settings = (EnvironmentSettings.new_instance(
).in_batch_mode().use_blink_planner().build())
t_env = BatchTableEnvironment.create(environment_settings=environment_settings)
t_env.get_config().get_configuration().set_integer(
    "table.exec.resource.default-parallelism", 6
)
# "asin","category","title","brand","rank","main_cat"
t_env.connect(
    FileSystem().path("/opt/examples/data/input/products_no_duplicates.csv")
).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("asin", DataTypes.STRING())
    .field("category", DataTypes.STRING())
    .field("brand", DataTypes.STRING())
    .field("rank", DataTypes.STRING())
    .field("main_cat", DataTypes.STRING())
).with_schema(
    Schema()
    .field("asin", DataTypes.STRING())
    .field("category", DataTypes.STRING())
    .field("brand", DataTypes.STRING())
    .field("rank", DataTypes.STRING())
    .field("main_cat", DataTypes.STRING())
).create_temporary_table(
    "products"
)
t_env.connect(FileSystem().path(
    "/opt/examples/data/input/customers.csv")).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("customer_id", DataTypes.STRING())
    .field("first_name", DataTypes.STRING())
    .field("last_name", DataTypes.STRING())
    .field("gender", DataTypes.STRING())
    .field("ssn", DataTypes.STRING())
    .field("credit_card", DataTypes.STRING())
    .field("credit_card_provider", DataTypes.STRING())
    .field("birth_date", DataTypes.STRING())
    .field("start_date", DataTypes.STRING())
    .field("title", DataTypes.STRING())
    .field("office", DataTypes.STRING())
    .field("organization", DataTypes.STRING())
    .field("salary", DataTypes.INT())
    .field("bonus", DataTypes.INT())
    .field("accured_holidays", DataTypes.INT())
).with_schema(
    Schema()
    .field("customer_id", DataTypes.STRING())
    .field("first_name", DataTypes.STRING())
    .field("last_name", DataTypes.STRING())
    .field("gender", DataTypes.STRING())
    .field("ssn", DataTypes.STRING())
    .field("credit_card", DataTypes.STRING())
    .field("credit_card_provider", DataTypes.STRING())
    .field("birth_date", DataTypes.STRING())
    .field("start_date", DataTypes.STRING())
    .field("title", DataTypes.STRING())
    .field("office", DataTypes.STRING())
    .field("organization", DataTypes.STRING())
    .field("salary", DataTypes.INT())
    .field("bonus", DataTypes.INT())
    .field("accured_holidays", DataTypes.INT())
).create_temporary_table(
    "customers"
)

t_env.connect(
    FileSystem().path("/opt/examples/data/input/transactions.csv")
).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("transaction_id", DataTypes.STRING())
    .field("product_id", DataTypes.STRING())
    .field("transaction_amount", DataTypes.DOUBLE())
    .field("transaction_date", DataTypes.STRING())
    .field("customer_id", DataTypes.STRING())
).with_schema(
    Schema()
    .field("transaction_id", DataTypes.STRING())
    .field("product_id", DataTypes.STRING())
    .field("transaction_amount", DataTypes.DOUBLE())
    .field("transaction_date", DataTypes.STRING())
    .field("customer_id", DataTypes.STRING())
).create_temporary_table(
    "transactions"
)

t_env.connect(
    FileSystem().path("/opt/examples/data/output/8_join_output.csv")
).with_format(Csv().derive_schema()).with_schema(
    Schema()
    .field("full_name", DataTypes.STRING())
    .field("brand", DataTypes.STRING())
    .field("main_cat", DataTypes.STRING())
    .field("transactions", DataTypes.STRING())
    .field("first_transaction_date", DataTypes.STRING())
    .field("avg_transaction", DataTypes.DOUBLE())
    .field("min_transaction", DataTypes.DOUBLE())
    .field("max_transaction", DataTypes.DOUBLE())
    .field("avg_salary", DataTypes.DOUBLE())
    .field("avg_bonus", DataTypes.DOUBLE())
).create_temporary_table(
    "mySink"
)

final_table = t_env.sql_query(
    """ select
            MIN(first_name) || ' ' ||  MIN(last_name)  as full_name,
            LISTAGG(distinct p.brand, ';') as product_brand,
            LISTAGG(distinct p.main_cat, ';') as main_cat,
            LISTAGG(transaction_id,';') as transactions,
            MIN(transaction_date) as first_transaction_date,
            avg(transaction_amount) as avg_transaction,
            min(transaction_amount) as min_transaction,
            max(transaction_amount) as max_transaction,
            avg(cast(salary as double)) as avg_salary,
            avg(cast(bonus as double)) as avg_bonus
            from transactions t left join customers c
            on t.customer_id=c.customer_id
            left join products p on t.product_id = p.asin
            group by c.customer_id"""
)
final_table.insert_into("mySink")
t_env.execute("8-join")
