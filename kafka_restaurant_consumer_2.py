import argparse
import certifi
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

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



def main(topic):
    print("inside main")

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # print(schema_registry_client.get_latest_version('Restaurant_Data'))

    subjects = schema_registry_client.get_subjects()

    schema_str = schema_registry_client.get_latest_version(subjects[0]).schema.schema_str

    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    count = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                print('No of records consumed by consumer 2: ' + str(count))
                continue

            resto = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if resto is not None:
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), resto))
                count+=1
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")