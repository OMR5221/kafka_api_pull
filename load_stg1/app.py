import os
import json
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
S1_DATA_TOPIC = os.environ.get('DATA_TOPIC')
"""CREATE A NEW TOPIC FOR EACH TAG TYPE?"""
S1_LOAD_TOPIC = os.environ.get('S1_LOAD_TOPIC')

def load(s1_data: dict):
    """Pivot the data record using pandas"""
    s1_df = pd.DataFrame(s1_data, index=[0])
    s1_df[s1_df["PLANT_ID", "TIMESTAMPLOCAL", "VALUE", "TAG_NAME"]]
    return s1_df


if __name__ == '__main__':
    s1_load_consumer = KafkaConsumer(
        S1_DATA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    s1_data_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for message in consumer:
        s1_data: dict = message.value
        producer.send(S1_LOAD_TOPIC, value=s1_data)
        print(S1_LOAD_TOPIC, s1_data)  # DEBUG
