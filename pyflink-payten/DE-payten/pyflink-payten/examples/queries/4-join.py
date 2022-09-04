from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, Csv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(6)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

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
    FileSystem().path("/opt/examples/data/output/4_join_output.csv")
).with_format(Csv().derive_schema()).with_schema(
    Schema()
    .field("avg_ta", DataTypes.DOUBLE())
    .field("min_ta", DataTypes.DOUBLE())
    .field("max_ta", DataTypes.DOUBLE())
    .field("sum_ta", DataTypes.DOUBLE())
).create_temporary_table(
    "mySink"
)

final_table = t_env.sql_query(
    """select
    avg(transaction_amount) as avg_ta,
    min(transaction_amount) min_ta,
    max(transaction_amount) max_ta,
    sum(transaction_amount) sum_ta
    from transactions
    """
)
final_table.insert_into("mySink")
t_env.execute("4-join")
