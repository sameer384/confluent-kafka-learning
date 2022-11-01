import argparse
import csv

import datetime

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

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

    my_schema = schema_registry_client.get_latest_version(topic + '-value').schema.schema_str
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=None)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    
    counter = 0
    with open('./output.csv', 'a+', newline='') as f:
        w = csv.writer(f)
        #go to the starting of the file
        f.seek(0)
        #if file is new (empty) just add the header record, we can get this header list from schema too
        if len(f.readlines())==0:
            f.seek(0)
            w.writerow(['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products'])

            

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            print(restaurant)

            if restaurant is not None:
                counter += 1
                print("User record {}: restaurant details: {}\n"
                      .format(msg.key(), restaurant))



             #create a list with column values of individual rows and insert to the file
                rowList = []
                for col in restaurant.values():
                    rowList.append(col)

                with open('./output.csv', 'a+', newline='') as f:
                    w = csv.writer(f)
                    w.writerow(rowList)

                print('Total messages fetched till now:', counter)
                
            
                        
            
        except KeyboardInterrupt:
            break

    consumer.close()


main("restaurant-take-away-data")