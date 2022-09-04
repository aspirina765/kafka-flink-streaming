from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, Csv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(6)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

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
        "/opt/examples/data/output/2_word_count_lovelike_output.csv")
).with_format(Csv().derive_schema()).with_schema(
    Schema().field("reviewText", DataTypes.STRING()).field("counts",
                                                           DataTypes.INT())
).create_temporary_table(
    "mySink"
)

# query
final_table = t_env.sql_query(
    """
    SELECT
    reviewText,
    (CHAR_LENGTH(reviewText)-
     CHAR_LENGTH(REPLACE(reviewText, 'like', ''))) / char_length('like') +
    (CHAR_LENGTH(reviewText)-
    CHAR_LENGTH(REPLACE(reviewText, 'love', ''))) / char_length('love')
    FROM mySource
    """
)

final_table.insert_into("mySink")
t_env.execute("2-word_count_lovelike")
