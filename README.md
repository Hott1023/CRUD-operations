# CRUD-operations
Execution of CRUD operations by Spark

## Узлы Docker контейнера и их порты
### Узлы работы со Spark
spark-master: 7077
spark-worker-1: 9081
### Узлы работы с Kafka и SchemaRegistry
kafka-control-center: 9021
broker: 9092
schema-registry: 8081
zookeeper: 2181
kafka-tools
### Узлы работы с HDFS
namenode: 9870
datanode: 9864
resourcemanager
nodemanager1
historyserver
### Узлы работы с Hive
hive-server: 10000
hive-metastore: 9083
hive-metastore-postgresql
presto-coordinator: 8089

## Команда параметры запсука скриптов:
Для запуска командного окна pyspark использовалась команда
```
/spark/bin/pyspark --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0 --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.hadoop.parquet.enable.summary-metadata=false'
```

## Скрипты для обработки CRUD операций
### from_kafka_to_delta_inline_processing
Скрипт для потоковй передачи из Kafka в Delta с встроенной обработки CRUD

### from_kafka_to_parquet_post_processing
kafka_parquet_sender.py - Скрипт для потоковй передачи из Kafka в буферную таблицу parquet

parquet_crud_processing.py - Скрипт для обработки запросов из буферной таблицы parquet и применения их в конечную таблицу.

## Тестовые скрипты для запуска из spark-консоли (TestScriptsForInlineCRUDProcessing):
В rebuild_crud_df представлено тестовое применение CRUD операций внутри DataFrame посредством SQL запросов

В rebuild_crud_df_by_for представлено тестовое применение CRUD операций внутри DataFrame посредством перебора в цикле

### Тестовый набор данных:
```
+---------+---+---+------+------+
|operation| ts|key|value1|value2|
+---------+---+---+------+------+
|        i|  1|  1|     1|     1|
|        u|  2|  1|     2|  null|
|        d|  3|  1|  null|  null|
|        u|  4|  2|     1|  null|
|        u|  5|  2|  null|     1|
|        d|  2|  3|  null|  null|
|        i|  1|  4|     1|     3|
|        u|  2|  4|  null|     8|
+---------+---+---+------+------+
```
