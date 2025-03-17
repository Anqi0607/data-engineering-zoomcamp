from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            test_data INTEGER,
            event_timestamp TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name



def create_events_source_kafka(t_env):
    # table只会在
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            -- 将event_timestamp处理成带有时区的timestamp TIMESTAMP_LTZ, 3表示精度
            -- as 后面为计算该column的方法
            -- event_watermark 的主要目的是为 watermark 的生成提供依据，它实际上并不是一个物理列，而是一个计算列或逻辑列
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            -- WATERMARK for ... as ... 用于定义 watermark
            -- 设置watermark为等待5秒, 五秒之前的event不会参与计算
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'test-topic',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    # 设置执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # 设置 Table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # create Kafka source table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        # 将 Kafka 中的记录写入 Postgres
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                test_data,
                -- 由于之前在source table中定义的event_watermark不是一个物理列,因此在query的时候需要写清楚如何处理event_timestamp
                -- 而不是直接select event_watermark column
                TO_TIMESTAMP_LTZ(event_timestamp, 3) as event_timestamp
            FROM {source_table}
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
