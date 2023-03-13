import csv
import json
from datetime import datetime

from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from delta import *
from delta.tables import *

producer_config = {
    "bootstrap.servers": "localhost:9092",
    "schema.registry.url": "http://localhost:8081"
}

TOPIC = 'CRUD_pipeline'


def main():
    key_schema = {
        "fields": [
            {
                "name": "key1",
                "type": "string"
            },
            {
                "name": "key2",
                "type": "string"
            },
            {
                "name": "key3",
                "type": "string"
            }
        ],
        "name": "key_CRUD_pipeline",
        "type": "record"
    }
    value_schema = {
        "fields": [
            {
                "name": "o",
                "type": "string"
            },
            {
                "name": "key1",
                "type": "string"
            },
            {
                "name": "key2",
                "type": "string"
            },
            {
                "name": "key3",
                "type": "string"
            },
            {
                "name": "value1",
                "type": [
                    "string",
                    "null"
                ]
            },
            {
                "name": "value2",
                "type": [
                    "string",
                    "null"
                ]
            },
            {
                "name": "value3",
                "type": [
                    "string",
                    "null"
                ]
            }
        ],
        "name": "value_CRUD_pipeline",
        "type": "record"
    }

    producer = AvroProducer(producer_config,
                            default_key_schema=json.dumps(key_schema),
                            default_value_schema=json.dumps(value_schema))
    with open('input/CRUD-operations_input.csv', 'r', newline='', encoding='utf-8') as file_in:
        reader = csv.DictReader(file_in, delimiter=';')
        for row in reader:

            key = {'key1': row['key1'],
                   'key2': row['key2'],
                   'key3': row['key3']}
            value = {'o': row['o'],
                     'key1': row['key1'],
                     'key2': row['key2'],
                     'key3': row['key3'],
                     'value1': None if row['value1'] == '' else row['value1'],
                     'value2': None if row['value2'] == '' else row['value2'],
                     'value3': None if row['value3'] == '' else row['value3']}
            for field in key.keys():
                value[field] = key[field]

            producer.produce(topic=TOPIC, key=key,
                             value=value)
            producer.flush()


if __name__ == '__main__':
    main()
