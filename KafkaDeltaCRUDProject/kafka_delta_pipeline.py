from pyspark.sql import SparkSession
from delta import *
from delta.tables import *
import json
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import struct
from pyspark.sql.functions import when
from pyspark.sql.functions import coalesce
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

CRUD_ACTION_COLUMN_NAME = 'o'
INSERT_COMMAND = 'i'
UPDATE_COMMAND = 'u'
DELETE_COMMAND = 'd'
UPSERT_COMMAND = 'ups'
SCHEMA_REGISTRY_ADDR = "http://schema-registry:8081"
TOPIC = 'CRUD_pipeline'
BOOTSTRAP_SERVER = 'broker:29092'
delta_table_name = f'{TOPIC}_delta_table'
checkpointLocation = f"/checkpoints/delta_{TOPIC}"
delta_table_location = f'hdfs://namenode:9000/delta-tables/{delta_table_name}/'
IS_DEBUG = True


class KafkaDeltaPipelineClass:
    def __init__(self):
        self.key_schema = None
        self.value_schema = None
        self.load_kafka_schemas()
        self.key_fields = None
        self.value_fields = None
        self.load_kafka_schema_fields()
        self.spark = None
        self.deltaTable = None
        self.init_spark()
        self.init_delta_table()

        self.key_condition = ''
        self.all_updating_conditions = []
        self.all_updating_sets = []
        self.delete_condition = ''
        self.delete_set = {}
        self.existing_insert_condition = ''
        self.existing_insert_set = {}
        self.non_existing_insert_condition = ''
        self.non_existing_insert_set = {}
        self.prepare_delta_uploading_conditions_and_sets()

    def load_kafka_schemas(self):
        schema_registry_conf = {'url': SCHEMA_REGISTRY_ADDR}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        self.key_schema = schema_registry_client.get_latest_version(f'{TOPIC}-key').schema.schema_str
        self.value_schema = schema_registry_client.get_latest_version(f'{TOPIC}-value').schema.schema_str

    def load_kafka_schema_fields(self):
        self.key_fields = set(map(lambda field: field['name'], json.loads(self.key_schema)['fields']))
        self.value_fields = set(map(lambda field: field['name'], json.loads(self.value_schema)['fields'])) - {
            CRUD_ACTION_COLUMN_NAME} - self.key_fields

    def init_spark(self):
        builder = SparkSession.builder.appName("DeltaWriterApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def init_delta_table(self):
        self.deltaTable = DeltaTable.createIfNotExists(self.spark) \
            .tableName(delta_table_name) \
            .addColumn('_is_row_deleted', 'BOOLEAN') \
            .addColumn('ts', 'TIMESTAMP')
        for key_field in self.key_fields:
            self.deltaTable = self.deltaTable.addColumn(key_field, 'STRING')
        for value_field in self.value_fields:
            self.deltaTable = self.deltaTable.addColumn(value_field, 'STRING')
        self.deltaTable = self.deltaTable.location(delta_table_location).execute()

    def get_kafka_stream(self):
        return self.spark \
            .readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
            .option("subscribe", TOPIC) \
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
                        *map(lambda x: f'value.{x} as {x}', self.value_fields))

    def prepare_df_for_rebuild(self, df):
        key_columns = map(lambda x: col(x), self.key_fields)
        value_columns = map(lambda x: col(x), self.value_fields)
        struct_df = df \
            .withColumn('key', struct(*key_columns)) \
            .withColumn('value', struct(*value_columns))
        prepared_df = struct_df \
            .withColumn('updating_value',
                        when((col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND), None)
                        .when((col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), None)
                        .when((col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), col('value'))) \
            .withColumn('inserting_value',
                        when((col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND), col('value'))
                        .when((col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), None)
                        .when((col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), None)) \
            .select(col(CRUD_ACTION_COLUMN_NAME), col('key'), col('ts'),
                    col('updating_value'), col('inserting_value')).orderBy(col('ts'))
        return prepared_df

    @staticmethod
    def get_prepared_df_with_unique_keys(prepared_df):
        return prepared_df.dropDuplicates(['key'])

    def enrich_end_df_with_row_key_equality(self, end_df, row):
        return end_df.selectExpr(CRUD_ACTION_COLUMN_NAME, 'key', 'ts',
                                 'updating_value', 'inserting_value',
                                 ' AND '.join(map(lambda x: f"key.{x} = '{row['key'][x]}'",
                                                  self.key_fields)) + ' as _key_eq')

    @staticmethod
    def enrich_temporary_df_by_delete_command_with_new_operation(temporary_df, row):
        return temporary_df \
            .withColumn('new_operation',
                        when(col('_key_eq'), DELETE_COMMAND)
                        .otherwise(col(CRUD_ACTION_COLUMN_NAME)))

    @staticmethod
    def enrich_temporary_df_by_delete_command_with_new_updating_value(temporary_df, row):
        return temporary_df \
            .withColumn('new_updating_value',
                        when(col('_key_eq'), lit(None))
                        .otherwise(col('updating_value')))

    @staticmethod
    def enrich_temporary_df_by_delete_command_with_new_inserting_value(temporary_df, row):
        return temporary_df \
            .withColumn('new_inserting_value',
                        when(col('_key_eq'), lit(None))
                        .otherwise(col('inserting_value')))

    def enrich_temporary_df_by_delete_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new_operation(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new_updating_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_delete_command_with_new_inserting_value(temporary_df, row)
        return temporary_df

    @staticmethod
    def enrich_temporary_df_by_insert_command_with_new_operation(temporary_df, row):
        return temporary_df \
            .withColumn('new_operation',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND), INSERT_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), UPSERT_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), UPSERT_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND), UPSERT_COMMAND)
                        .otherwise(col(CRUD_ACTION_COLUMN_NAME)))

    def enrich_temporary_df_by_insert_command_with_new_updating_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_updating_value',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND), lit(None))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND),
                              struct(*map(lambda x: lit(row['inserting_value'][x]).alias(x),
                                          self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND),
                              col('updating_value'))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND),
                              col('updating_value'))
                        .otherwise(col('updating_value')))

    def enrich_temporary_df_by_insert_command_with_new_inserting_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_inserting_value',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND),
                             col('inserting_value'))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND),
                              struct(*map(lambda x: lit(row['inserting_value'][x]).alias(x),
                                          self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND),
                              struct(*map(lambda x: lit(row['inserting_value'][x]).alias(x),
                                          self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND),
                              col('inserting_value'))
                        .otherwise(col('inserting_value')))

    def enrich_temporary_df_by_insert_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new_operation(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new_updating_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_insert_command_with_new_inserting_value(temporary_df, row)
        return temporary_df

    @staticmethod
    def enrich_temporary_df_by_update_command_with_new_operation(temporary_df, row):
        return temporary_df \
            .withColumn('new_operation',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND), UPSERT_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), DELETE_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), UPDATE_COMMAND)
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND), UPSERT_COMMAND)
                        .otherwise(col(CRUD_ACTION_COLUMN_NAME)))

    def enrich_temporary_df_by_update_command_with_new_updating_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_updating_value',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND),
                             struct(*map(lambda x: lit(row['updating_value'][x]).alias(x),
                                         self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), lit(None))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND),
                              struct(*map(lambda x: coalesce(lit(row['updating_value'][x]),
                                                             col(f'updating_value.{x}')).alias(x),
                                          self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND),
                              struct(*map(lambda x: coalesce(lit(row['updating_value'][x]),
                                                             col(f'updating_value.{x}')).alias(x),
                                          self.value_fields)))
                        .otherwise(col('updating_value')))

    def enrich_temporary_df_by_update_command_with_new_inserting_value(self, temporary_df, row):
        return temporary_df \
            .withColumn('new_inserting_value',
                        when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == INSERT_COMMAND),
                             struct(*map(lambda x: coalesce(lit(row['updating_value'][x]),
                                                            col(f'inserting_value.{x}')).alias(x),
                                         self.value_fields)))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == DELETE_COMMAND), lit(None))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPDATE_COMMAND), lit(None))
                        .when(col('_key_eq') & (col(CRUD_ACTION_COLUMN_NAME) == UPSERT_COMMAND),
                              struct(*map(lambda x: coalesce(lit(row['updating_value'][x]),
                                                             col(f'inserting_value.{x}')).alias(x),
                                          self.value_fields)))
                        .otherwise(col('inserting_value')))

    def enrich_temporary_df_by_update_command(self, temporary_df, row):
        temporary_df = self.enrich_temporary_df_by_update_command_with_new_operation(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_update_command_with_new_updating_value(temporary_df, row)
        temporary_df = self.enrich_temporary_df_by_update_command_with_new_inserting_value(temporary_df, row)
        return temporary_df

    @staticmethod
    def enrich_temporary_df_with_new_ts(temporary_df, row):
        return temporary_df.withColumn('new_ts',
                                       when(col('_key_eq'), lit(row['ts']))
                                       .otherwise(col('ts'))) \
            .select(col('new_operation').alias(CRUD_ACTION_COLUMN_NAME), col('key'), col('new_ts').alias('ts'),
                    col('new_updating_value').alias('updating_value'),
                    col('new_inserting_value').alias('inserting_value'))

    def unpack_rebuild_df_and_prepare_before_upload(self, end_df):
        return end_df.selectExpr(CRUD_ACTION_COLUMN_NAME, *map(lambda x: f'key.{x}', self.key_fields), 'ts',
                                 *map(lambda x: f'updating_value.{x} as updating_{x}', self.value_fields),
                                 *map(lambda x: f'inserting_value.{x} as inserting_{x}', self.value_fields),
                                 f"({CRUD_ACTION_COLUMN_NAME} = '{DELETE_COMMAND}') as _is_row_deleted")

    def rebuild_crud_df(self, df):
        prepared_df = self.prepare_df_for_rebuild(df)
        end_df = self.get_prepared_df_with_unique_keys(prepared_df)
        for row in prepared_df.rdd.toLocalIterator():
            temporary_df = self.enrich_end_df_with_row_key_equality(end_df, row)
            if row[CRUD_ACTION_COLUMN_NAME] == DELETE_COMMAND:
                temporary_df = self.enrich_temporary_df_by_delete_command(temporary_df, row)
            elif row[CRUD_ACTION_COLUMN_NAME] == INSERT_COMMAND:
                temporary_df = self.enrich_temporary_df_by_insert_command(temporary_df, row)
            elif row[CRUD_ACTION_COLUMN_NAME] == UPDATE_COMMAND:
                temporary_df = self.enrich_temporary_df_by_update_command(temporary_df, row)
            else:
                continue
            end_df = self.enrich_temporary_df_with_new_ts(temporary_df, row)
            if IS_DEBUG:
                end_df.show()
        return self.unpack_rebuild_df_and_prepare_before_upload(end_df)

    def prepare_delta_key_condition(self):
        self.key_condition = ' and '.join([f't.{key_field} = s.{key_field}' for key_field in self.key_fields])

    def prepare_all_updating_conditions_and_upload_sets(self):
        for key in range(0, 2 ** len(self.value_fields) - 1):
            updating_condition = f"t._is_row_deleted = false and " \
                                 f"(s.{CRUD_ACTION_COLUMN_NAME} = '{UPDATE_COMMAND}' or " \
                                 f"s.{CRUD_ACTION_COLUMN_NAME} = '{UPSERT_COMMAND}')"
            updating_set = {'ts': 's.ts'}
            for value_field in self.value_fields:
                if key % 2 == 0:
                    updating_condition += f" and s.updating_{value_field} is not null"
                    updating_set[value_field] = f's.updating_{value_field}'
                else:
                    updating_condition += f" and s.updating_{value_field} is null"
                key = key // 2
            self.all_updating_conditions.append(updating_condition)
            self.all_updating_sets.append(updating_set)

    def prepare_delete_condition_and_upload_set(self):
        self.delete_condition = f"s.{CRUD_ACTION_COLUMN_NAME} = '{DELETE_COMMAND}'"
        self.delete_set = {'ts': 's.ts',
                           '_is_row_deleted': 's._is_row_deleted'}

    def prepare_existing_insert_condition_and_upload_set(self):
        self.existing_insert_condition = f"t._is_row_deleted = true and " \
                                         f"(s.{CRUD_ACTION_COLUMN_NAME} = '{INSERT_COMMAND}' or " \
                                         f"s.{CRUD_ACTION_COLUMN_NAME} = '{UPSERT_COMMAND}')"
        self.existing_insert_set = {'ts': 's.ts',
                                    '_is_row_deleted': 's._is_row_deleted'}
        for key_field in self.key_fields:
            self.existing_insert_set[key_field] = f's.{key_field}'
        for value_field in self.value_fields:
            self.existing_insert_set[value_field] = f's.inserting_{value_field}'

    def prepare_non_existing_insert_condition_and_upload_set(self):
        self.non_existing_insert_condition = f"s.{CRUD_ACTION_COLUMN_NAME} = '{INSERT_COMMAND}' or " \
                                             f"s.{CRUD_ACTION_COLUMN_NAME} = '{UPSERT_COMMAND}'"
        self.non_existing_insert_set = {'ts': 's.ts',
                                        '_is_row_deleted': 's._is_row_deleted'}
        for key_field in self.key_fields:
            self.non_existing_insert_set[key_field] = f's.{key_field}'
        for value_field in self.value_fields:
            self.non_existing_insert_set[value_field] = f's.inserting_{value_field}'

    def prepare_delta_uploading_conditions_and_sets(self):
        self.prepare_delta_key_condition()
        self.prepare_all_updating_conditions_and_upload_sets()
        self.prepare_delete_condition_and_upload_set()
        self.prepare_existing_insert_condition_and_upload_set()
        self.prepare_non_existing_insert_condition_and_upload_set()

    def upload_stream_to_delta(self, stream):
        def crud_operation_delta(microBatchOutputDF, batchId):
            if IS_DEBUG:
                microBatchOutputDF.show()
            rebuild_df = self.rebuild_crud_df(microBatchOutputDF)
            crud_operation = self.deltaTable.alias("t").merge(rebuild_df.alias("s"), self.key_condition)
            crud_operation = crud_operation.whenNotMatchedInsert(condition=self.non_existing_insert_condition,
                                                                 values=self.non_existing_insert_set)
            crud_operation = crud_operation.whenMatchedUpdate(condition=self.existing_insert_condition,
                                                              set=self.existing_insert_set)
            crud_operation = crud_operation.whenMatchedUpdate(condition=self.delete_condition,
                                                              set=self.delete_set)
            for i in range(0, 2 ** len(self.value_fields) - 1):
                crud_operation = crud_operation.whenMatchedUpdate(condition=self.all_updating_conditions[i],
                                                                  set=self.all_updating_sets[i])
            crud_operation.execute()
            if IS_DEBUG:
                rebuild_df.show()
                self.deltaTable.toDF().show()

        stream.writeStream \
            .format("delta") \
            .foreachBatch(crud_operation_delta) \
            .outputMode("update") \
            .option('checkpointLocation', checkpointLocation) \
            .start(delta_table_location) \
            .awaitTermination()

    def start_pipeline(self):
        self.upload_stream_to_delta(self.get_represent_kafka_stream())


def main():
    KafkaDeltaPipelineClass().start_pipeline()


if __name__ == '__main__':
    main()
