import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import (
    StreamTableEnvironment,
    CsvTableSink,
    DataTypes,
    EnvironmentSettings,
)
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka
from pyflink.table.window import Tumble


def transaction_job():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(
        s_env,
        environment_settings=EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .use_blink_planner()
        .build(),
    )

    register_transactions_source(st_env)
    register_transaction_sink(st_env)

    st_env.from_path("source").window(
        Tumble.over("30.seconds").on("rowtime").alias("w")
    ).group_by("w").select(
        "sum(transaction_amount) as sum_ta, "
        "count(transaction_amount), "
        "w.end as maxtime"
    ).insert_into(
        "sink"
    )

    st_env.execute("9-window_stream")


def register_transactions_source(st_env):
    st_env.connect(
        Kafka()
        .version("universal")
        .topic("transactions")
        .start_from_latest()
        .property("zookeeper.connect", "zookeeper:2181")
        .property("bootstrap.servers", "kafka-broker:9092")
    ).with_format(
        Json()
        .fail_on_missing_field(True)
        .schema(
            DataTypes.ROW(
                [
                    DataTypes.FIELD("transaction_amount", DataTypes.DOUBLE()),
                    DataTypes.FIELD("transaction_date", DataTypes.TIMESTAMP()),
                ]
            )
        )
    ).with_schema(
        Schema()
        .field("transaction_amount", DataTypes.DOUBLE())
        .field("rowtime", DataTypes.TIMESTAMP())
        .rowtime(
            Rowtime()
            .timestamps_from_field("transaction_date")
            .watermarks_periodic_bounded(30000)
        )
    ).in_append_mode().register_table_source(
        "source"
    )


def register_transaction_sink(st_env):
    result_file = "/opt/examples/data/output/9_window_stream.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env.register_table_sink(
        "sink",
        CsvTableSink(
            ["sum_ta", "count_ta", "maxtime"],
            [DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.TIMESTAMP()],
            result_file,
        ),
    )


if __name__ == "__main__":
    transaction_job()