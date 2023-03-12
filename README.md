# CRUD-operations
Execution of CRUD operations by Spark

## Скрипты для запуска из spark-консоли:
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
