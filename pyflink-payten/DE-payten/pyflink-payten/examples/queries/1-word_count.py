from pyflink.table import (
    TableConfig,
    DataTypes,
    BatchTableEnvironment,
    EnvironmentSettings,
)

INPUT_TABLE = "mySource"
OUTPUT_TABLE = "mySink"

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

ddl_source = f"""
       CREATE TABLE {INPUT_TABLE} (
         `word` STRING
       ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'csv.field-delimiter' = ' ',
        'csv.disable-quote-character' = 'true',
        'csv.ignore-parse-errors' = 'true',
        'path' = '/opt/examples/data/input/words1.csv'
    )
   """

ddl_sink = f"""
       CREATE TABLE {OUTPUT_TABLE} (
        `word` STRING,
        `count` BIGINT
       ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/opt/examples/data/output/1_word_count_output'
    )
   """

t_env.execute_sql(ddl_source)
t_env.execute_sql(ddl_sink)

t_env.from_path(INPUT_TABLE).group_by("word").select(
    "word, count(1) as count"
).insert_into(OUTPUT_TABLE)

t_env.execute("1-word_count")
