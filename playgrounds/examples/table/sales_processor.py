import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import StreamingFileSink


# def tutorial():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     ds = env.from_collection(
#         collection=[(1, 'aaa'), (2, 'bbb')],
#         type_info=Types.ROW([Types.INT(), Types.STRING()]))
#     ds.add_sink(StreamingFileSink
#                 .for_row_format('./tmp/output', Encoder.simple_string_encoder())
#                 .build())
#     env.execute("tutorial_job")

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance() \
                      .in_streaming_mode() \
                      .use_blink_planner() \
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')

    # .set_string("execution.checkpointing.interval", "10s")
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))\
            .set_string("execution.checkpointing.interval", "10s")

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE sales_usd (
            seller_id VARCHAR,
            amount_usd DOUBLE,
            sale_ts BIGINT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-usd',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sales-usd',
            'format' = 'csv'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sales_usd')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          seller_id,
          TUMBLE_END(proctime, INTERVAL '1' SECONDS) AS window_end,
          SUM(amount_usd) * 0.85 AS window_sales
        FROM sales_usd
        GROUP BY
          TUMBLE(proctime, INTERVAL '1' SECONDS),
          seller_id
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()
    

    # ###############################################################
    # # Create Kafka Sink Table
    # ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            seller_id VARCHAR,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'csv'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()
    print(revenue_tbl.to_pandas())

    tbl_env.execute_sql(source_ddl) 

    tbl_env.execute('windowed-sales-euros').print()

    ##

    # tbl_env1 = revenue_tbl

    # tbl_env1.execute_sql(source_ddl) 

    # tbl1 = tbl_env1.from_path("windowed-sales-euros")

    # print(tbl1.to_pandas()) ### or tbl.limit(10).execute().print()
    # tbl1.limit(10).execute().print()


from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json

import os 


def from_kafka_to_kafka_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)

    # use blink table planner
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')

    # .set_string("execution.checkpointing.interval", "10s")
    st_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))\
            .set_string("execution.checkpointing.interval", "10s")

    # register source and sink
    register_rides_source(st_env)
    register_rides_sink(st_env)

    # query
    st_env.from_path("source-stock").insert_into("sink")

    # execute
    st_env.execute("3-from_kafka_to_kafka")



# "timestamp": 1611799200,
# "open": 0.9797100000000001,
# "high": 0.97996,
# "low": 0.97874,
# "close": 0.9790000000000001,
# "volume": 30128
def register_rides_source(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("universal")
            .topic("stock")
            .start_from_earliest()
            .property("zookeeper.connect", "zookeeper:2181")
            .property("bootstrap.servers", "kafka:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("priceId", DataTypes.BIGINT()),
            DataTypes.FIELD("timestamp", DataTypes.STRING()),
            DataTypes.FIELD("open", DataTypes.FLOAT()),
            DataTypes.FIELD("high", DataTypes.FLOAT()),
            DataTypes.FIELD("low", DataTypes.FLOAT()),
            DataTypes.FIELD("close", DataTypes.FLOAT()),
            DataTypes.FIELD("volume", DataTypes.BIGINT())]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("priceId", DataTypes.BIGINT())
            .field("timestamp", DataTypes.STRING())
            .field("open", DataTypes.FLOAT())
            .field("high", DataTypes.FLOAT())
            .field("low", DataTypes.FLOAT())
            .field("close", DataTypes.FLOAT())
            .field("volume", DataTypes.BIGINT())) \
        .in_append_mode() \
        .create_temporary_table("source-stock")


def register_rides_sink(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("universal")
            .topic("StockResults")
            .property("zookeeper.connect", "zookeeper:2181")
            .property("bootstrap.servers", "kafka:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("priceId", DataTypes.BIGINT()),
            DataTypes.FIELD("timestamp", DataTypes.STRING()),
            DataTypes.FIELD("open", DataTypes.FLOAT()),
            DataTypes.FIELD("high", DataTypes.FLOAT()),
            DataTypes.FIELD("low", DataTypes.FLOAT()),
            DataTypes.FIELD("close", DataTypes.FLOAT()),
            DataTypes.FIELD("volume", DataTypes.BIGINT())
        ]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("priceId", DataTypes.BIGINT())
            .field("timestamp", DataTypes.STRING())
            .field("open", DataTypes.FLOAT())
            .field("high", DataTypes.FLOAT())
            .field("low", DataTypes.FLOAT())
            .field("close", DataTypes.FLOAT())
            .field("volume", DataTypes.BIGINT())) \
        .in_append_mode() \
        .create_temporary_table("sink")


if __name__ == '__main__':
    from_kafka_to_kafka_demo()

# if __name__ == '__main__':
#     main()