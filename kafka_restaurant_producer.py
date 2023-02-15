import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "F:/BIG DATA CLASS RESOURCES/Confluent-Kafka-Setup-main/restaurant_orders.csv"
columns=['Order_Number','Order_Date','Item_Name','Quantity','Product_Price','Total_products']

API_KEY = 'RU2P2ZLFACBWKEBL'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw0d1.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'CGevlIsGBBTE1R30ht/pshKLJ5sKfQoYdxzjkoToX5mp1S9dkcHbn/wI0Qdnc5zd'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'EKZZJMWTWR5IJQ7D'
SCHEMA_REGISTRY_API_SECRET = 'ZnV2U+C+IIy7xaX6UYwTvycVQ+rRQkaU6RxndTcXc12OwXM18zEJpBQ5lL7plh3H'

def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
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

class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def get_restaurant_instance(file_path):
    df=pd.read_csv(file_path)
    # df=df.iloc[:,1:]
    restaurants:List[Restaurant]=[]
    for data in df.values:
        restaurant=Restaurant(dict(zip(columns,data)))
        restaurants.append(restaurant)
        yield restaurant

def restaurant_to_dict(restaurant:Restaurant, ctx):
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
    # print(schema_registry_client.get_latest_version('Restaurant_Data'))

    subjects = schema_registry_client.get_subjects()

    schema_str = schema_registry_client.get_latest_version(subjects[0]).schema.schema_str


    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurant_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for resto in get_restaurant_instance(file_path=FILE_PATH):

            print(resto)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), restaurant_to_dict),
                            value=json_serializer(resto, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            # break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurent-take-away-data")