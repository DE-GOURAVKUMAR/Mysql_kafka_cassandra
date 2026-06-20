import time
from datetime import datetime
import mysql.connector
import pandas as pd

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# ==========================================
# 1. KAFKA & SCHEMA REGISTRY CONFIGURATION
# ==========================================
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'FPZDFHY44L5CRCIM',
    'sasl.password': 'cflt/JB5fJ7/MVGqseBaEnufG8NNg6uGVzThyvWqRJ6SCBf619NqMoqy+pJ5ZdXA'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-zgrkm29.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('7JPIYRRZDT5LONFB','cfltZEbZpqiG35Fkn8hncks/FdaFVu3bqSRtOP18ZhEJb5B2OmVmN1jrv3FsRdug')
})

# Fetch the Avro schema registered under the topic value subject
subject_name = 'topic_0-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Initialize translators
key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema_str)

# Initialize the stateful Serializing Producer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': value_serializer
})

# ==========================================
# 2. TRACKING CALLBACK & INITIAL STATE
# ==========================================
# This variable acts as our system watermark. 
# It tracks the 'last_updated' timestamp from the database.
# Initialize it to the beginning of time (or a historical timestamp).
last_read_timestamp = "1970-01-01 00:00:00"

def delivery_report(err, msg):
    """
    Callback triggered automatically when Kafka confirms successful delivery.
    """
    if err is not None:
        print(f"Delivery failed for Product Key {msg.key()}: {err}")
    else:
        print(f"Product {msg.key()} committed successfully to partition [{msg.partition()}] at offset {msg.offset()}")

# ==========================================
# 3. CONTINUOUS INGESTION PIPELINE
# ==========================================
print(f"Engine Online. Tracking changes since: {last_read_timestamp}")

while True:
    try:
        # Establish connection inside the loop to recover automatically from network timeouts
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="Pass@8989#",
            database="kafka"
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Query rule: Fetch records strictly GREATER than our last watermark timestamp
            # Ordered sequentially by last_updated to advance our watermark chronologically
            query = """
                SELECT id, name, category, price, last_updated 
                FROM product 
                WHERE last_updated > %s 
                ORDER BY last_updated ASC;
            """
            cursor.execute(query, (last_read_timestamp,))
            rows = cursor.fetchall()

            cursor.close()
            connection.close()

            # Process rows if new appends or mutations exist in the DB
            if rows:
                print(f"\n[Database Alert] Found {len(rows)} modified or new product records.")
                
                # Load into pandas for structural mapping and cleansing
                df = pd.DataFrame(rows)
                df = df.fillna('null')

                for index, row in df.iterrows():
                    data_value = row.to_dict()

                    # Explicit primitive casting to comply with fastavro limitations
                    data_value["id"] = int(data_value["id"])
                    data_value["price"] = float(data_value["price"])
                    data_value["name"] = str(data_value["name"])
                    data_value["category"] = str(data_value["category"])
                    
                    # Convert datetimes from MySQL to a unified string representation
                    if isinstance(data_value["last_updated"], datetime):
                        data_value["last_updated"] = data_value["last_updated"].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        data_value["last_updated"] = str(data_value["last_updated"])

                    # Extract properties for clear routing definition
                    product_id_key = str(data_value["id"])
                    current_record_timestamp = data_value["last_updated"]

                    # Produce payload to cloud cluster
                    producer.produce(
                        topic='topic_0',
                        key=product_id_key,  # Product ID key ensures partitioning mapping stability
                        value=data_value,
                        on_delivery=delivery_report
                    )
                    
                    # Force transmission immediately over the wire
                    producer.flush()

                    # UPDATE WATERMARK: Move the tracking timestamp forward after this record is processed
                    last_read_timestamp = current_record_timestamp
                    print(f"Watermark advanced to: {last_read_timestamp}")

            else:
                # No data variations discovered, log heartbeat tick
                print(".", end="", flush=True)

    except mysql.connector.Error as db_err:
        print(f"\n[MySQL Error]: {db_err}. Retrying pipeline linkage shortly...")
    except Exception as e:
        print(f"\n[Pipeline Engine Exception]: {e}")

    # Query throttling interval: Poll database for changes every 5 seconds
    time.sleep(2)