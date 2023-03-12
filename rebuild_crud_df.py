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
	        col('updating_value'), col('inserting_value'), col('is_deleted'))
end_df = prepared_df.dropDuplicates(['key'])
working_df = prepared_df

def update_end_df(df, end_df):
	counted_df = df.groupBy(col('key')).agg(count('*').alias('counts'))
	new_end_rows = counted_df.filter(col('counts') == 1).alias('s1').join(df.alias('s2'), (col('s1.key') == col('s2.key'))) \
		.select(col('s2.operation').alias('operation'), col('s1.key').alias('key'), col('s2.ts').alias('ts'), 
				col('s2.updating_value').alias('updating_value'), col('s2.inserting_value').alias('inserting_value'))
	return end_df.alias('s1').join(new_end_rows.alias('s2'), col('s1.key') == col('s2.key'), 'left') \
		.withColumn('new_operation', coalesce(col('s2.operation'), col('s1.operation'))) \
		.withColumn('new_ts', coalesce(col('s2.ts'), col('s1.ts'))) \
		.withColumn('new_updating_value', coalesce(col('s2.updating_value'), col('s1.updating_value'))) \
		.withColumn('new_inserting_value', coalesce(col('s2.inserting_value'), col('s1.inserting_value'))) \
		.select(col('new_operation').alias('operation'), 
				col('s1.key').alias('key'), 
				col('new_ts').alias('ts'), 
				col('new_updating_value').alias('updating_value'), 
				col('new_inserting_value').alias('inserting_value'))

def filter_df(df):
	counted_df = df.groupBy(col('key')).agg(count('*').alias('counts'))
	return counted_df.filter(col('counts') > 1).alias('s1').join(df.alias('s2'), (col('s1.key') == col('s2.key'))) \
		.select(col('s2.operation').alias('operation'), col('s1.key').alias('key'), col('s2.ts').alias('ts'), 
				col('s2.updating_value').alias('updating_value'), col('s2.inserting_value').alias('inserting_value'),
				col('s2.is_deleted').alias('is_deleted'))

def unite_df(df):
	joined_df = df.alias('s1').join(df.alias('s2'), (col('s1.key') == col('s2.key')) & (col('s1.ts') < col('s2.ts')))
	balanced_joined_df = joined_df.withColumn('new_operation', 
			when((col('s1.operation') == 'i') & (col('s2.operation') == 'i'), 'i') \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'd'), 'd') \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'u'), 'ups') \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'ups'), 'ups') \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'u'), 'd') \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'd'), 'd') \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'i'), 'ups') \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'ups'), 'ups') \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'd'), 'd') \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'i'), 'ups') \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'u'), 'u') \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'ups'), 'ups') \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'i'), 'ups') \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'd'), 'd') \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'u'), 'ups') \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'ups'), 'ups')) \
		.withColumn('new_updating_value', 
			when((col('s1.operation') == 'i') & (col('s2.operation') == 'i'), None) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'u'), col('s2.updating_value')) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'ups'), col('s2.updating_value')) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'u'), None) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'i'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'ups'), col('s2.updating_value')) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'i'), col('s1.updating_value')) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'u'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.updating_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.updating_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'ups') & col('s2.is_deleted'), col('s2.updating_value')) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'ups') & ~col('s2.is_deleted'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.updating_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.updating_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'i'), col('s1.updating_value')) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'u'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.updating_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.updating_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'ups') & col('s2.is_deleted'), col('s2.updating_value')) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'ups') & ~col('s2.is_deleted'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.updating_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.updating_value.value2')).alias('value2')))) \
		.withColumn('new_inserting_value', 
			when((col('s1.operation') == 'i') & (col('s2.operation') == 'i'), col('s1.inserting_value')) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'u'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.inserting_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.inserting_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'ups') & col('s2.is_deleted'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'i') & (col('s2.operation') == 'ups') & ~col('s2.is_deleted'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.inserting_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.inserting_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'u'), None) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'i'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'd') & (col('s2.operation') == 'ups'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'i'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'u'), None) \
			.when((col('s1.operation') == 'u') & (col('s2.operation') == 'ups'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'i'), col('s1.inserting_value')) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'd'), None) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'u'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.inserting_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.inserting_value.value2')).alias('value2'))) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'ups') & col('s2.is_deleted'), col('s2.inserting_value')) \
			.when((col('s1.operation') == 'ups') & (col('s2.operation') == 'ups') & ~col('s2.is_deleted'), 
				  struct(coalesce(col('s2.updating_value.value1'), col('s1.inserting_value.value1')).alias('value1'),
				         coalesce(col('s2.updating_value.value2'), col('s1.inserting_value.value2')).alias('value2')))) \
		.withColumn('new_is_deleted', (col('s1.is_deleted') | col('s2.is_deleted'))) \
		.select(col('new_operation').alias('operation'), col('s1.key').alias('key'), 
	             col('s1.ts').alias('firstOpTS'), col('s2.ts').alias('lastOpTS'), 
	             col('new_updating_value').alias('updating_value'), col('new_inserting_value').alias('inserting_value'), 
				 col('new_is_deleted').alias('is_deleted'))
	balanced_joined_df.show()
	grouped_joined_df = joined_df.select(col('s1.key').alias('key'), col('s1.ts').alias('firstOpTS'), col('s2.ts').alias('lastOpTS')) \
		.groupBy(col('key'), col('lastOpTS')).agg(max(col('firstOpTS')).alias('firstOpTS'))
	return grouped_joined_df.alias('s1').join(balanced_joined_df.alias('s2'), (col('s1.key') == col('s2.key')) & 
				 (col('s1.lastOpTS') == col('s2.lastOpTS')) & 
				 (col('s1.firstOpTS') == col('s2.firstOpTS'))) \
		.select(col('s2.operation').alias('operation'), col('s1.key').alias('key'), 
				col('s1.lastOpTS').alias('ts'), 
				col('s2.updating_value').alias('updating_value'), col('s2.inserting_value').alias('inserting_value'), 
				col('s2.is_deleted').alias('is_deleted'))

working_df.show()
end_df.show()
end_df = update_end_df(working_df, end_df)
working_df = filter_df(working_df)
working_df.show()
working_df = unite_df(working_df)
working_df.show()
end_df.show()

end_df = update_end_df(working_df, end_df)
working_df = filter_df(working_df)
working_df.show()
working_df = unite_df(working_df)
working_df.show()
end_df.show()

end_df = update_end_df(working_df, end_df)
working_df = filter_df(working_df)
working_df.show()
working_df = unite_df(working_df)
working_df.show()
end_df.show()

