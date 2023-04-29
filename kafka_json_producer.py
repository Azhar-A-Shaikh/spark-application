
import argparse
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import mysql.connector
import datetime,time
from typing import List
import pandas as pd

sql = "SELECT * FROM orders_data"
columns = ["order_id", "customer_id", "item_id","order_date", "item_value", "quantity", "total_amount"]

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="spark_streaming"
)

API_KEY = 'VIMWNFMLY7IRCNTU'
API_SECRET_KEY = 'bWtf9sqCyFnzO+9wgXUylVQCPTDm2owwtgXKjvK7XNfPHV4ecPSRWUGf6HRTsXpY'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL  = 'https://psrc-6y63j.ap-southeast-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'W3FWYC527DOMPNE2'
SCHEMA_REGISTRY_API_SECRET = 'JCZcpeOd7GL4KlZlISM35CmCU0/ygAghG1Yzytxt8Zxp/ivLgWqor/u7QfqVs6Oe'

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    }

class Car:   
    def __init__(self, record: dict):
        self.order_id = record["order_id"]
        self.customer_id = record["customer_id"]
        self.item_id = record["item_id"]
        self.order_date = record["order_date"]
        self.item_value = record["item_value"]
        self.quantity = record["quantity"]
        self.total_amount = record["total_amount"]
        self.record = record

    @staticmethod
    def dict_to_car(data: dict, ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"

def car_to_dict(car: Car, ctx):
    return {
        "order_id": car.order_id,
        "customer_id": car.customer_id,
        "item_id": car.item_id,
        "order_date": str(car.order_date),  # Convert to string
        "item_value": car.item_value,
        "quantity": car.quantity,
        "total_amount": car.total_amount
    }



def get_car_instance(sql):
    df = pd.read_sql(sql, con=mydb)
    cars: List[Car] = []
    for data in df.values:
        data_list = list(data)
        car = Car(dict(zip(columns, data_list)))
        cars.append(car)
        yield car


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = topic + '-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str = schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)

    producer = Producer(sasl_conf())

    print("Producing records to topic {}. ^C to exit.".format(topic))

    producer.poll(0.0)
    try:
        for car in get_car_instance(sql):
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4())),
                             value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main("orders_data")