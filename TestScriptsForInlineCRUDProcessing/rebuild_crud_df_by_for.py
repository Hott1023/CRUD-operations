from pyspark.sql import SparkSession
from delta import *
from delta.tables import *
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.csv("/test.csv", sep=";", inferSchema=True, header=True)
df.show()
structed_df = df.withColumn('value', struct(col('value1'), col('value2')))
prepared_df = structed_df.withColumn('updating_value', 
		when((col('operation') == 'i'), None) \
		.when((col('operation') == 'd'), None) \
		.when((col('operation') == 'u'), col('value'))) \
	.withColumn('inserting_value', 
		when((col('operation') == 'i'), col('value')) \
		.when((col('operation') == 'd'), None) \
		.when((col('operation') == 'u'), None)) \
	.withColumn('is_deleted', (col('operation') == 'd')) \
	.select(col('operation'), col('key'), col('ts'), 
			col('updating_value'), col('inserting_value'), col('is_deleted')).orderBy(col('ts'))
end_df = prepared_df.dropDuplicates(['key'])
for row in prepared_df.rdd.toLocalIterator():
	print(row)
	if row['operation'] == 'd':
		temporary_df = end_df.withColumn('new_operation', 
				when((col('key') == lit(row['key'])), 'd')
				.otherwise(col('operation'))) \
			.withColumn('new_updating_value',
				when((col('key') == lit(row['key'])), lit(None))
				.otherwise(col('updating_value'))) \
			.withColumn('new_inserting_value',
				when((col('key') == lit(row['key'])), lit(None))
				.otherwise(col('inserting_value'))) \
			.withColumn('new_is_deleted', 
				when((col('key') == lit(row['key'])), lit(True))
				.otherwise(col('is_deleted')))
	elif row['operation'] == 'i':
		temporary_df = end_df.withColumn('new_operation', 
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), 'i')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), 'ups')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), 'ups')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), 'ups')
				.otherwise(col('operation'))) \
			.withColumn('new_updating_value',
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), lit(None))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), 
					struct(lit(row['inserting_value']['value1']).alias('value1'), 
					       lit(row['inserting_value']['value2']).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), col('updating_value'))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), col('updating_value'))
				.otherwise(col('updating_value'))) \
			.withColumn('new_inserting_value',
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), col('inserting_value'))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), 
					struct(lit(row['inserting_value']['value1']).alias('value1'), 
					       lit(row['inserting_value']['value2']).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), 
					struct(lit(row['inserting_value']['value1']).alias('value1'), 
					       lit(row['inserting_value']['value2']).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), col('inserting_value'))
				.otherwise(col('inserting_value'))) \
			.withColumn('new_is_deleted', col('is_deleted'))
	elif row['operation'] == 'u':
		temporary_df = end_df.withColumn('new_operation', 
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), 'ups')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), 'd')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), 'u')
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), 'ups')
				.otherwise(col('operation'))) \
			.withColumn('new_updating_value',
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), 
					struct(lit(row['updating_value']['value1']).alias('value1'), 
						   lit(row['updating_value']['value2']).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), lit(None))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), 
					struct(coalesce(lit(row['updating_value']['value1']), col('updating_value.value1')).alias('value1'),
				           coalesce(lit(row['updating_value']['value2']), col('updating_value.value2')).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), 
					struct(coalesce(lit(row['updating_value']['value1']), col('updating_value.value1')).alias('value1'),
				           coalesce(lit(row['updating_value']['value2']), col('updating_value.value2')).alias('value2')))
				.otherwise(col('updating_value'))) \
			.withColumn('new_inserting_value',
				when((col('key') == lit(row['key'])) & (col('operation') == 'i'), 
					struct(coalesce(lit(row['updating_value']['value1']), col('inserting_value.value1')).alias('value1'),
				           coalesce(lit(row['updating_value']['value2']), col('inserting_value.value2')).alias('value2')))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'd'), lit(None))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'u'), lit(None))
				.when((col('key') == lit(row['key'])) & (col('operation') == 'ups'), 
					struct(coalesce(lit(row['updating_value']['value1']), col('inserting_value.value1')).alias('value1'),
				           coalesce(lit(row['updating_value']['value2']), col('inserting_value.value2')).alias('value2')))
				.otherwise(col('inserting_value'))) \
			.withColumn('new_is_deleted', col('is_deleted'))
	end_df = temporary_df.withColumn('new_ts',
			when((col('key') == lit(row['key'])), lit(row['ts']))
			.otherwise(col('ts'))) \
		.select(col('new_operation').alias('operation'), col('key'), col('new_ts').alias('ts'), 
	             col('new_updating_value').alias('updating_value'), col('new_inserting_value').alias('inserting_value'), 
				 col('new_is_deleted').alias('is_deleted'))
	end_df.show()

