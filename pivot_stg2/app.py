import os
import json
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
S1_DATA_TOPIC = os.environ.get('DATA_TOPIC')
"""CREATE A NEW TOPIC FOR EACH TAG TYPE?"""
S2_PIVOT_TOPIC = os.environ.get('PIVOT_TOPIC')

def pivot(s1_data: dict):
    """Pivot the data record using pandas"""
    s1_df = pd.DataFrame(s1_data, index=[0])

    s1_df = pd.pivot_table(s1_df, index=['PLANT_ID', 'TIMESTAMPLOCAL'],
			  columns='TAG_TYPE', values="VALUE",
	                  aggfunc=[sum])

    if s1_df['PLANT_ID'] = 503:
        s1_df_503 = pd.loc(s1_df['PLANT_ID'] == 503)
        # Need to multiply volt and current tag types and sum again against inverter sums:
        s1_df_503['VOLT_CUR'] = -1 * s1_df_503['INDIV_VOLT'] * s1_df_503['INDIV_CURR'] / 1000
        # SUM(s1_df_503['VOLT_CURR'], s1_df_503['INDIV_INPUT_POWER'])
        s1_df_503['SITE_POWER'] = s1_df_503['SITE_POWER'] + s1_df_503['VOLT_CUR']

    return s1_df.to_json()

if __name__ == '__main__':
    consumer = KafkaConsumer(
        S1_DATA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for message in consumer:
        s1_data: dict = message.value
        s2_pivot = pivot(s1_data)
        producer.send(S2_PIVOT_TOPIC, value=s2_pivot)
        print(S2_PIVOT_TOPIC, s2_pivot)  # DEBUG
