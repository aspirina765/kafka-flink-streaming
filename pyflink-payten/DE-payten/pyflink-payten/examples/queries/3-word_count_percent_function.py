import re
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.udf import udf

# from pyflink.table.udf import udaf

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)
t_env.get_config().get_configuration().set_string(
    "taskmanager.memory.task.off-heap.size", "80m"
)


@udf(input_types=DataTypes.STRING(), result_type=DataTypes.INT())
def percent_count(text):
    return len(re.findall("""\\d+(?:\\.\\d+)?%""", text))


t_env.register_function("percent_count", percent_count)

t_env.connect(
    FileSystem().path("/opt/examples/data/input/products_with_pipe.csv")
).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter("|")
    .field("overall", DataTypes.STRING())
    .field("verified", DataTypes.STRING())
    .field("asin", DataTypes.STRING())
    .field("reviewText", DataTypes.STRING())
    .field("summary", DataTypes.STRING())
).with_schema(
    Schema()
    .field("overall", DataTypes.STRING())
    .field("verified", DataTypes.STRING())
    .field("asin", DataTypes.STRING())
    .field("reviewText", DataTypes.STRING())
    .field("summary", DataTypes.STRING())
).create_temporary_table(
    "mySource"
)

t_env.connect(
    FileSystem().path(
        "/opt/examples/data/output/3_word_count_percent_function_output.csv"
    )
).with_format(
    OldCsv().field("reviewText", DataTypes.STRING()).field("counts",
                                                           DataTypes.INT())
).with_schema(
    Schema().field("reviewText", DataTypes.STRING()).field("counts",
                                                           DataTypes.INT())
).create_temporary_table(
    "mySink"
)

t_env.from_path("mySource").select(
    "reviewText, percent_count(reviewText)").insert_into("mySink")
t_env.execute("3-word_count_percent-function1")
