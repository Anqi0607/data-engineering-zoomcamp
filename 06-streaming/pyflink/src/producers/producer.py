from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Kafka
from pyflink.datastream.formats import Json
from pyflink.table import StreamTableEnvironment
from pyflink.table.descriptors import Kafka, OldCsv, Schema
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction
from pyflink.table.connectors import JDBC

# 创建 StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# 创建 TableEnvironment
t_env = StreamTableEnvironment.create(env)

# 设置 Kafka 源
t_env.connect(
    Kafka()
    .version("universal")  # 可以根据需要设置 Kafka 的版本
    .topic("test-topic")
    .start_from_latest()
    .property("bootstrap.servers", "localhost:9092")
)
.with_format(Json())  # 消息格式为 JSON
.with_schema(Schema()
    .field("test_data", DataTypes.INT())
    .field("event_timestamp", DataTypes.TIMESTAMP()))  # 与 Kafka 中的消息结构一致
.create_temporary_table("kafka_source")

# 将 Kafka 消息转换为 Table
kafka_table = t_env.from_path("kafka_source")

# 设置 PostgreSQL 连接
t_env.connect(
    JDBC()
    .url("jdbc:postgresql://localhost:5432/postgres")
    .table("processed_events")
    .driver("org.postgresql.Driver")
    .username("postgres")
    .password("postgres")
)
.with_schema(Schema()
    .field("test_data", DataTypes.INT())
    .field("event_timestamp", DataTypes.TIMESTAMP()))
.create_temporary_table("postgres_sink")

# 将 Kafka 数据插入到 PostgreSQL 表中
kafka_table.execute_insert("postgres_sink").wait()

# 启动作业
env.execute("Flink Kafka to PostgreSQL Job")
