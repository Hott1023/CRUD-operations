from pyspark.sql import SparkSession
import json
from hdfs import InsecureClient
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import struct
from pyspark.sql.functions import when
from pyspark.sql.functions import coalesce
from confluent_kafka.schema_registry import SchemaRegistryClient

from pyspark.sql.utils import AnalysisException

CRUD_ACTION_COLUMN_NAME = 'o'
INSERT_COMMAND = 'i'
UPDATE_COMMAND = 'u'
DELETE_COMMAND = 'd'
UPSERT_COMMAND = 'ups'
END_COMMAND = 'end'

SCHEMA_REGISTRY_ADDR = "http://schema-registry:8081"
TOPIC = 'CRUD_pipeline'
HDFS_PATH = f'http://namenode:9870/'

IS_DEBUG = True


class CRUDProcessing:
    def __init__(self, topic, hdfs_path, schema_registry_addr):
        self.topic = topic
        self.hdfs_path = hdfs_path
        self.schema_registry_conf = {'url': schema_registry_addr}

        self.parquet_table_name = f'{TOPIC}_parquet_table'
        self.buffer_parquet_table_location = f'{self.hdfs_path}parquet-tables/buffer_{self.parquet_table_name}/'
        self.hdfs_buffer_path = f'/parquet-tables/buffer_{self.parquet_table_name}/'
        self.end_copy_parquet_table_location = f'{self.hdfs_path}parquet-tables/end_copy_{self.parquet_table_name}/'
        self.hdfs_end_copy_path = f'/parquet-tables/end_copy_{self.parquet_table_name}/'
        self.end_parquet_table_location = f'{self.hdfs_path}parquet-tables/{self.parquet_table_name}/'

        self.key_schema = None
        self.value_schema = None
        self.load_kafka_schemas()
        self.key_fields = None
        self.value_fields = None
        self.load_kafka_schema_fields()

        self.spark = None
        self.init_spark()

    def load_kafka_schemas(self):
        schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
        self.key_schema = schema_registry_client.get_latest_version(f'{TOPIC}-key').schema.schema_str
        self.value_schema = schema_registry_client.get_latest_version(f'{TOPIC}-value').schema.schema_str

    def load_kafka_schema_fields(self):
        self.key_fields = set(map(lambda field: field['name'], json.loads(self.key_schema)['fields']))
        self.value_fields = set(map(lambda field: field['name'], json.loads(self.value_schema)['fields'])) - {
            CRUD_ACTION_COLUMN_NAME} - self.key_fields

    def init_spark(self):
        spark_session = SparkSession \
            .builder \
            .appName("ParquetCRUDProcessing") \
            .getOrCreate()
        self.spark = spark_session

    def get_prepared_df(self, df):
        key_columns = map(lambda x: col(x), self.key_fields)
        value_columns = map(lambda x: col(x), self.value_fields)
        struct_df = df \
            .withColumn('key', struct(*key_columns)) \
            .withColumn('value', struct(*value_columns)) \
            .select(col(CRUD_ACTION_COLUMN_NAME), col('ts'), col('key'),
                    col('value'), col('_is_deleted'))
        return struct_df

    def get_undeleted_rows(self, prepared_df):
        return prepared_df.alias('s1') \
            .join(prepared_df.alias('s2'),
                  (col('s1.key') == col('s2.key')) &
                  (((col('s1.ts') < col('s2.ts')) &
                    (col('s2._is_deleted') | (col(f's2.{CRUD_ACTION_COLUMN_NAME}') == END_COMMAND))) |
                   ((col('s1.ts') == col('s2.ts')) &
                    (col(f's1.{CRUD_ACTION_COLUMN_NAME}') != END_COMMAND) &
                    (col(f's2.{CRUD_ACTION_COLUMN_NAME}') == END_COMMAND))), "left_anti")

    def create_end_df(self, df):
        return df.dropDuplicates(['key']) \
            .withColumn('_is_deleted',
                        when(col('_is_deleted') | (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), lit(True))
                        .otherwise(col('_is_deleted'))) \
            .withColumn(CRUD_ACTION_COLUMN_NAME,
                        when(lit(END_COMMAND).isNotNull(), lit(END_COMMAND)).otherwise(lit(None)))

    def enrich_end_df_with_row_key_equality(self, end_df, row):
        return end_df \
            .selectExpr(CRUD_ACTION_COLUMN_NAME, 'ts', 'key', 'value', '_is_deleted',
                        ' AND '.join(map(lambda x: f"key.{x} = '{row['key'][x]}'", self.key_fields)) + ' as _key_eq')

    def enrich_temporary_df_by_delete_command_with_new__is_deleted(self, temporary_df):
        return temporary_df \
            .withColumn('new__is_deleted',
                        when(col('_key_eq'), lit(True))
                        .otherwise(col('_is_deleted')))

    def enrich_temporary_df_by_delete_command_with_new_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_value',
                        when(col('_key_eq'), lit(None))
                        .otherwise(col('value')))

    def enrich_temporary_df_by_delete_command_with_new_ts(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_ts',
                        when(col('_key_eq'), lit(row['ts']))
                        .otherwise(col('ts')))

    def enrich_temporary_df_by_delete_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new__is_deleted(temporary_df)
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new_ts(temporary_df, row)
        return temporary_df

    def enrich_temporary_df_by_insert_command_with_new__is_deleted(self, temporary_df):
        return temporary_df \
            .withColumn('new__is_deleted',
                        when(col('_key_eq') & col('_is_deleted'), lit(False))
                        .otherwise(col('_is_deleted')))

    def enrich_temporary_df_by_insert_command_with_new_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_value',
                        when(col('_key_eq') & col('_is_deleted'),
                             struct(*map(lambda x: lit(row['value'][x]).alias(x),
                                         self.value_fields)))
                        .otherwise(col('value')))

    def enrich_temporary_df_by_insert_command_with_new_ts(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_ts',
                        when(col('_key_eq') & col('_is_deleted'), lit(row['ts']))
                        .otherwise(col('ts')))

    def enrich_temporary_df_by_insert_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new__is_deleted(temporary_df)
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new_ts(temporary_df, row)
        return temporary_df

    def enrich_temporary_df_by_update_command_with_new__is_deleted(self, temporary_df):
        return temporary_df.withColumn('new__is_deleted', col('_is_deleted'))

    def enrich_temporary_df_by_update_command_with_new_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_value',
                        when(col('_key_eq') & ~col('_is_deleted'),
                             struct(*map(lambda x: coalesce(lit(row['value'][x]),
                                                            col(f'value.{x}')).alias(x), self.value_fields)))
                        .otherwise(col('value')))

    def enrich_temporary_df_by_update_command_with_new_ts(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_ts',
                        when(col('_key_eq') & ~col('_is_deleted'), lit(row['ts']))
                        .otherwise(col('ts')))

    def enrich_temporary_df_by_update_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_update_command_with_new__is_deleted(temporary_df)
        temporary_df = self.enrich_temporary_df_by_update_command_with_new_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_update_command_with_new_ts(temporary_df, row)
        return temporary_df

    def represent_temporary_df_to_end(self, temporary_df):
        return temporary_df.select(col(CRUD_ACTION_COLUMN_NAME),
                                   col('new__is_deleted').alias('_is_deleted'),
                                   col('key'),
                                   col('new_ts').alias('ts'),
                                   col('new_value').alias('value'))

    def rebuild_df(self, parquet_df):
        prepared_df = self.get_undeleted_rows(self.get_prepared_df(parquet_df)).orderBy(col('ts'))
        end_df = self.create_end_df(prepared_df)
        for row in prepared_df.rdd.toLocalIterator():
            temporary_df = self.enrich_end_df_with_row_key_equality(end_df, row)
            if IS_DEBUG:
                print(row)
                temporary_df.show()
            if row[CRUD_ACTION_COLUMN_NAME] == DELETE_COMMAND:
                temporary_df = self.enrich_temporary_df_by_delete_command(temporary_df, row)
            elif row[CRUD_ACTION_COLUMN_NAME] == INSERT_COMMAND:
                temporary_df = self.enrich_temporary_df_by_insert_command(temporary_df, row)
            elif row[CRUD_ACTION_COLUMN_NAME] == UPDATE_COMMAND:
                temporary_df = self.enrich_temporary_df_by_update_command(temporary_df, row)
            else:
                continue
            end_df = self.represent_temporary_df_to_end(temporary_df)
        if IS_DEBUG:
            end_df.show()
        return end_df

    def unpack_rebuild_df_and_prepare_before_upload(self, end_df):
        return end_df.selectExpr(CRUD_ACTION_COLUMN_NAME, 'ts', *map(lambda x: f'key.{x} as {x}', self.key_fields),
                                 *map(lambda x: f'value.{x} as {x}', self.value_fields),
                                 '_is_deleted')

    def get_list_buffer_parquet_parts(self):
        client = InsecureClient(self.hdfs_path, user='user')
        return filter(lambda x: (x.find('part') == 0) and (x.find('.parquet') == len(x) - 8),
                      client.list(self.hdfs_buffer_path))

    def get_union_parquet_df(self):
        buffer_parquet_df = self.spark.read.format("parquet").load(self.buffer_parquet_table_location + "*.parquet")
        try:
            end_parquet_df = self.spark.read.parquet(self.end_copy_parquet_table_location)
        except AnalysisException:
            return buffer_parquet_df
        return end_parquet_df.union(buffer_parquet_df)

    def get_new_end_df(self):
        union_parquet_df = self.get_union_parquet_df()
        if IS_DEBUG:
            union_parquet_df.show()
            self.get_undeleted_rows(self.get_prepared_df(union_parquet_df)).orderBy(col('ts')).show()
        new_end_df = self.rebuild_df(union_parquet_df)
        return self.unpack_rebuild_df_and_prepare_before_upload(new_end_df)

    def write_df_to_end_table(self, df):
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(self.end_parquet_table_location)

    def delete_buffer_parquet_parts(self, parts):
        client = InsecureClient(self.hdfs_path, user='user')
        for part in parts:
            client.delete(self.hdfs_buffer_path + part)

    def create_end_copy_df(self):
        try:
            end_parquet_df = self.spark.read.parquet(self.end_parquet_table_location)
            end_parquet_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .save(self.end_copy_parquet_table_location)
        except AnalysisException:
            pass

    def delete_end_copy_df(self):
        client = InsecureClient(self.hdfs_path, user='user')
        client.delete(self.hdfs_end_copy_path, recursive=True)

    def processing(self):
        try:
            self.create_end_copy_df()
            buffer_parquet_parts = self.get_list_buffer_parquet_parts()
            new_end_df = self.get_new_end_df()
            if IS_DEBUG:
                new_end_df.show()
            self.write_df_to_end_table(new_end_df)
            self.delete_end_copy_df()
            self.delete_buffer_parquet_parts(buffer_parquet_parts)
        except AnalysisException as e:
            print(e)


def main():
    CRUDProcessing(topic=TOPIC, hdfs_path=HDFS_PATH, schema_registry_addr=SCHEMA_REGISTRY_ADDR)


if __name__ == '__main__':
    main()
