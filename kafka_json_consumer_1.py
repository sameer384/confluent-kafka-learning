import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

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
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


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
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    counter = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                counter += 1
                print("User record {}: restaurant details: {}\n"
                      .format(msg.key(), restaurant))
                print('Total messages fetched till now:', counter)
        except KeyboardInterrupt:
            break

    consumer.close()


main("restaurent-take-away-data")