import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "D:\work\kafka_class\sam\estaurant_orders.csv"
columns = ['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']

API_KEY = 'Q5RIU4N5PQHBNF5J'
ENDPOINT_SCHEMA_URL = 'https://psrc-mw0d1.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'rmFDN7p+ttkn/VwhYopH6aPuJTSoraerQkVPaY16fGvBMLe3GTPmWK8yfjmjRaEr'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'CH5IOR72N6OPZJ5O'
SCHEMA_REGISTRY_API_SECRET = 'Dxna1cDG7//pjd3RXsHFO60qtK55WdHooAO/B1nQzWEc3SXfeUj3xGMm4jxlX+79'

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Restaurant:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_restaurant(data: dict, ctx):
        return restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def get_order_details(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:,:]
    orders: List[Restaurant] = []
    for data in df.values:
        restaurant = Restaurant(dict(zip(columns, data)))
        orders.append(restaurant)
        yield restaurant


def order_to_dict(restaurant: Restaurant, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return restaurant.record


def order_report(err, msg):
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
    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "order_number": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "order_date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "item_name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "quantity": {
      "description": "The type(v) type is used.",
      "type": "number"  
    },
    "product_price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "total_products": {
      "description": "The type(v) type is used.",
      "type": "number"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, order_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for restaurant in get_order_details(file_path=FILE_PATH):
            print(restaurant)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), order_to_dict),
                             value=json_serializer(restaurant, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=order_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurent-take-away-data")