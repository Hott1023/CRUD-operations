from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

CRUD_ACTION_COLUMN_NAME = 'o'
INSERT_COMMAND = 'i'
UPDATE_COMMAND = 'u'
DELETE_COMMAND = 'd'
UPSERT_COMMAND = 'ups'

SCHEMA_REGISTRY_ADDR = "http://schema-registry:8081"
TOPIC = 'CRUD_pipeline'
HDFS_PATH = f'http://namenode:9870/'
BOOTSTRAP_SERVER = 'broker:29092'

IS_DEBUG = True


class KafkaParquetPipelineClass:
    def __init__(self, topic, hdfs_path, bootstrap_server, schema_registry_addr):
        self.topic = topic
        self.hdfs_path = hdfs_path
        self.schema_registry_conf = {'url': schema_registry_addr}
        self.bootstrap_server = bootstrap_server

        self.parquet_table_name = f'{self.topic}_parquet_table'
        self.buffer_parquet_table_location = f'{self.hdfs_path}parquet-tables/buffer_{self.parquet_table_name}/'
        self.parquet_checkpointLocation = f"/checkpoints/parquet_{self.topic}"

        self.key_schema = None
        self.value_schema = None
        self.load_kafka_schemas()
        self.key_fields = None
        self.value_fields = None
        self.load_kafka_schema_fields()

        self.spark = None
        self.deltaTable = None
        self.init_spark()

    def load_kafka_schemas(self):
        schema_registry_conf = {'url': SCHEMA_REGISTRY_ADDR}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        self.key_schema = schema_registry_client.get_latest_version(f'{self.topic}-key').schema.schema_str
        self.value_schema = schema_registry_client.get_latest_version(f'{self.topic}-value').schema.schema_str

    def load_kafka_schema_fields(self):
        self.key_fields = set(map(lambda field: field['name'], json.loads(self.key_schema)['fields']))
        self.value_fields = set(map(lambda field: field['name'], json.loads(self.value_schema)['fields'])) - {
            CRUD_ACTION_COLUMN_NAME} - self.key_fields

    def init_spark(self):
        spark_session = SparkSession \
            .builder \
            .appName("ParquetWriterApp") \
            .getOrCreate()
        self.spark = spark_session

    def get_kafka_stream(self):
        return self.spark \
            .readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", self.topic) \
            .option("mode", "PERMISSIVE") \
            .option("startingOffsets", 'earliest') \
            .load()

    def get_represent_kafka_stream(self):
        return self.get_kafka_stream() \
            .selectExpr("timestamp as ts", "substring(key, 6) as avro_key", "substring(value, 6) as avro_value") \
            .select(col("ts"), from_avro(col("avro_key"), self.key_schema).alias("key"),
                    from_avro(col("avro_value"), self.value_schema).alias("value")) \
            .selectExpr(f"value.{CRUD_ACTION_COLUMN_NAME} as {CRUD_ACTION_COLUMN_NAME}", "ts",
                        *map(lambda x: f'key.{x} as {x}', self.key_fields),
                        *map(lambda x: f'value.{x} as {x}', self.value_fields),
                        f"value.{CRUD_ACTION_COLUMN_NAME} = '{DELETE_COMMAND}' as _is_deleted")

    def upload_stream_to_parquet(self, stream):
        return stream.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", self.buffer_parquet_table_location) \
            .option('checkpointLocation', self.parquet_checkpointLocation) \
            .start()

    def start_parquet_pipeline(self):
        return self.upload_stream_to_parquet(self.get_represent_kafka_stream())


def main():
    KafkaParquetPipelineClass(topic=TOPIC, hdfs_path=HDFS_PATH,
                              bootstrap_server=BOOTSTRAP_SERVER, schema_registry_addr=SCHEMA_REGISTRY_ADDR) \
        .start_parquet_pipeline().awaitTermination()


if __name__ == '__main__':
    main()
