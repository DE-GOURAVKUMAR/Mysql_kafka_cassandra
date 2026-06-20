from decimal import *
from uuid import UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import mysql.connector
from mysql.connector import Error


def delivery_report(err, msg):
    """
    Callback triggered automatically when Kafka confirms successful delivery.
    """
    if err is not None:
        print(f"Delivery failed for Product Key {msg.key()}: {err}")
    else:
        print(f"Product {msg.key()} committed successfully to partition [{msg.partition()}] at offset {msg.offset()}")

kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'FPZDFHY44L5CRCIM',
    'sasl.password': 'cflt/JB5fJ7/MVGqseBaEnufG8NNg6uGVzThyvWqRJ6SCBf619NqMoqy+pJ5ZdXA'
}


# Create the schema registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-zgrkm29.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('7JPIYRRZDT5LONFB','cfltZEbZpqiG35Fkn8hncks/FdaFVu3bqSRtOP18ZhEJb5B2OmVmN1jrv3FsRdug')
})

# Fatch the latest avro schema
subject_name = 'topic_0-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema For Registry")
print(schema_str)

# create the avro serializer for the value
key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema_str)

# define the producer 
producer = SerializingProducer({
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': value_serializer
})


last_read_timestamp = "1970-01-01 00:00:00"
print(f"Engine Online. Tracking changes since: {last_read_timestamp}")
# while True:
while True:
    try: 
        if connection.is_connected():
            connection = mysql.connector.connect(
                host="127.0.0.1",
                user="root",
                password="Pass@8989#",
                database="kafka"
                )
        #  2. Create a cursor object
            cursor = connection.cursor(dictionary=True)
        # 3. Execute a query
            query = cursor.execute("SELECT * FROM product WHERE last_updated > %s ORDER BY id ASC;", (last_updated_id,))
            rows = cursor.fetchall()
# finally:
            cursor.close()
            connection.close()
#     # 5. Clean up resources
#     if 'connection' in locals() and connection.is_connected():
#         cursor.close()
#         connection.close()
#         print("MySQL connection is closed")

            df = pd.DataFrame(rows)
            df = df.fillna('null')
            for index, rows in df.iterrows():
                data_value = rows.to_dict()

                data_value['id']= int(data_value['id'])
                data_value['name']= str(data_value['name'])
                data_value['category']= str(data_value['category'])
                data_value['price']= float(data_value['price'])
                data_value['last_updated']= str(data_value['last_updated'])

                producer.produce(
                    topic='topic_0',
                    key= str(data_value['id']),
                    value= data_value,
                    on_delivery= delivery_report
                )
                producer.flush()
                last_updated_id = data_value['last_updated']
                
                print("My Sql Connection is closed")
                time.sleep(3)
    else:
        print(".", end="", flush=True )

except mysql.connector.Error as error:
    print(f"[DataBase Error] : {error}")
except Exception as e:
    print(f"System Error : {str(e)}")
