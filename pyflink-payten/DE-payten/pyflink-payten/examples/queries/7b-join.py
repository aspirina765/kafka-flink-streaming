from pyflink.table import (
    TableConfig,
    DataTypes,
    BatchTableEnvironment,
    EnvironmentSettings,
)

# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance()
    .in_batch_mode()
    .use_blink_planner()
    .build()
)
t_env.get_config().get_configuration().set_integer(
    "table.exec.resource.default-parallelism", 6
)

customers = """
    CREATE TABLE customers(
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        gender STRING,
        ssn STRING,
        credit_card STRING,
        credit_card_provider STRING,
        birth_date STRING,
        start_date STRING,
        title STRING,
        office STRING,
        organization STRING,
        salary INT,
        bonus INT,
        accured_holidays INT
    ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}',
        'csv.ignore-parse-errors' = 'true'
    )
    """.format(
    "/opt/examples/data/input/customers.csv"
)
transactions = """
    CREATE TABLE transactions(
        transaction_id STRING,
        product_id STRING,
        transaction_amount DOUBLE,
        transaction_date STRING,
        customer_id STRING
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}',
        'csv.ignore-parse-errors' = 'true'
    )
    """.format(
    "/opt/examples/data/input/transactions.csv"
)

sink_ddl = """
    CREATE TABLE mySink(
        first_name STRING,
        last_name STRING,
        avg_ta DOUBLE,
        avg_income INT,
        spending DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}'
    )
    """.format(
    "/opt/examples/data/output/7b_join_output.csv"
)

t_env.execute_sql(sink_ddl)
t_env.execute_sql(transactions)
t_env.execute_sql(customers)

final_table = t_env.sql_query(
    """select
        max(c.first_name),
        max(c.last_name),
        avg(transaction_amount) as avg_ta,
        avg(salary+bonus) as avg_income,
        avg(salary+bonus) - avg(transaction_amount) as spending
        from transactions t left join customers c
        on t.customer_id = c.customer_id
        group by c.customer_id
    """
)

final_table.insert_into("mySink")
t_env.execute("7b-join")
