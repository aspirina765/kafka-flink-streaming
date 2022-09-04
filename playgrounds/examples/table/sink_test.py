from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")

table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1'
    )
""")

table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/tmp/output',
        'sink.rolling-policy.rollover-interval' = '10s'
    )
""")

table_env.execute_sql("""
INSERT INTO print
SELECT id, data
FROM datagen
""").wait()