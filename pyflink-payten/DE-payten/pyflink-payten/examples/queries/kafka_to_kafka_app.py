from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import (
    TableConfig,
    DataTypes,
    StreamTableEnvironment,
    EnvironmentSettings,
)
from pyflink.table.window import Tumble
from pyflink.table.udf import udaf
# from pyflink.table import * 
# from pyflink.table.udf import * 

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
environment_settings = EnvironmentSettings.new_instance().\
    use_blink_planner().build()
t_env = StreamTableEnvironment.create(
    env, environment_settings=environment_settings)

INPUT_TABLE = "my_topic"
INPUT_TOPIC = "transactions"
LOCAL_KAFKA = "host.docker.internal:19092"
OUTPUT_TABLE = "my_topic_output"
OUTPUT_TOPIC = "my-topic-out"


@udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
def mean(x):
    return x.mean()


@udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
def sum(x):
    return x.sum()


@udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
def count(x):
    return x.count()


ddl_source = f"""
       CREATE TABLE {INPUT_TABLE} (
        client_id STRING,
        credit_card_number STRING,
        credit_card_provider STRING,
        credit_card_security_number STRING,
        company STRING,
        job STRING,
        transaction_amount DOUBLE,
        transaction_date TIMESTAMP(3),
        WATERMARK FOR transaction_date AS
        transaction_date - INTERVAL '60' SECOND
       ) WITH (
           'connector' = 'kafka',
           'topic' = '{INPUT_TOPIC}',
           'properties.bootstrap.servers' = '{LOCAL_KAFKA}',
           'format' = 'json'
       )
   """

ddl_sink = f"""
       CREATE TABLE {OUTPUT_TABLE} (
         sum_transaction_amount DOUBLE,
         count_transaction_amount DOUBLE,
         mean_transaction_amount DOUBLE,
         maxtime TIMESTAMP(3)
       ) WITH (
           'connector' = 'kafka',
           'topic' = '{OUTPUT_TOPIC}',
           'properties.bootstrap.servers' = '{LOCAL_KAFKA}',
           'format' = 'json'
       )
   """

t_env.get_config().get_configuration().set_integer(
    "python.fn-execution.bundle.size", 500
)
t_env.get_config().get_configuration().set_integer(
    "python.fn-execution.bundle.time", 1000
)
t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse",
                                                   True)

t_env.register_function("mean", mean)
t_env.register_function("sum", sum)
t_env.register_function("count", count)
t_env.execute_sql(ddl_source)
t_env.execute_sql(ddl_sink)

t_env.from_path("my_topic").window(
    Tumble.over("60.seconds").on("transaction_date").alias("w")
).group_by("w").select(
    "sum(transaction_amount) as sum_transaction_amount, "
    "count(transaction_amount) as count_transaction_amount, "
    "mean(transaction_amount) as mean_transaction_amount, "
    "w.end as maxtime"
).insert_into(
    "my_topic_output"
)

t_env.execute("kafka-to-kafka-app")
