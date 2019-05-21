"""Produce fake transactions into a Kafka topic."""
import threading, logging, time
import multiprocessing
import os
from time import sleep
from time import gmtime, strftime
from pytz import timezone
import json
from random import choices, randint
from string import ascii_letters, digits

from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey, select, func, Integer, String, DateTime)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (sessionmaker, Session)
from datetime import date, datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from pandas import DataFrame, merge
import pandas as pd
import requests
# Parse response for the needed values to load into STG1 table:
from bs4 import BeautifulSoup
import json
# import boto3
# from boto3 import Session
import uuid
# from botocore.config import Config
import requests
from kafka import KafkaProducer
import io
import random
import avro.schema
from avro.io import DatumWriter


DATA_TOPIC = os.environ.get('DATA_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
# TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 # / TRANSACTIONS_PER_SECOND

class EventProducer(threading.Thread):

    def __init__(self, db_url):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.topic = 'TEST' # set to plant_id.tagtype.tagname
        # Make DB Connections
        self.engine = create_engine(db_url)
        self.connection = self.engine.connect()
        # Call code to query and format api calls:
        self.config_data_df = self.config_data()
        # self.daterange_df = self.daterange()
        # self.run_data_df = self.run_data()

    def stop(self):
        self.stop_event.set()

    def engine(self, db_url):
        self.engine = create_engine(db_url)

    def connection(self, engine):
        self.db_connection = engine.connect()

    """Generate the time array: Want to use a Config file"""
    def backfill(self, INTERVAL=5, MINUTES=1440):

        minutes_in_timerange = INTERVAL * MINUTES
        today = datetime.utcnow().date()
        end_dt = datetime(today.year, today.month, today.day)
        # Get future values to request from Kakfka Producer:
        start_dt = end_dt - timedelta(days=INTERVAL)

        timerange_list = {i: start_dt + timedelta(minutes=i) for i in range(minutes_in_timerange)}

        dateranges = []

        for key, value in timerange_list.items():
            row = {}
            row['DateId'] = key
            row['DateStr'] = value.strftime('%Y-%m-%d %H:%M:%S')
            row['DateVal'] = value
            dateranges.append(row)

        # Push rows of Date dict into a DF
        return pd.DataFrame(dateranges)

    def config_data(self):

        # produce our own MetaData object
        metadata = MetaData()

        # Reflect the specified tables:
        metadata.reflect(self.engine, only=['es_tags_dim', 'es_plant_dim'])

        # we can then produce a set of mappings from this MetaData.
        Base = automap_base(metadata=metadata)

        # calling prepare() just sets up mapped classes and relationships.
        Base.prepare()

        # mapped classes are now created with names by default
        # matching that of the table name.
        es_tags_dim = Base.classes.es_tags_dim

        # Create DF from query:
        es_tags_df = pd.read_sql_query("""
                                        select
                                            td.TAG_ID,
                                            td.TAG_TYPE,
                                            td.TAG_NAME,
                                            td.PLANT_ID,
                                            pd.PLANT_CODE,
                                            td.SERVER_NAME,
                                            td.PI_METHOD
                                        from ES_TAGS_DIM td
                                        join ES_PLANT_DIM pd
                                            on pd.plant_id = td.plant_id
                                        WHERE pd.status = 'A'
                                        """, self.connection)

        # Update column names:
        es_tags_df.rename(columns=es_tags_df.iloc[0])
        es_tags_df.columns = map(str.upper, es_tags_df.columns)

        return es_tags_df

    def merge_time_tags(self, dt_df):
        # Merge the tags and time range dataframes:
        return self.config_data_df.assign(foo=1).merge(self.daterange_df.assign(foo=1)).drop('foo', 1)


    def call_api(self, rec, dts) -> dict:
        """Creates records from calling pjm api"""

        # tags_tf_df = merge_tag_tf()

        url = "http://dummy-api.url.com"
        headers = {'content-type': 'text/xml'}

        # Define a ditionary with the API Settings per type:
        rtr_pi_method_config = {
               'GetArchiveValue': """<?xml version="1.0" encoding="utf-8"?>
                   <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:fpl="http://fpl.pgen/">
                   <soapenv:Header/>
                   <soapenv:Body>
                      <fpl:GetArchiveValue>
                         <!--Optional:-->
                         <fpl:sServerName>{ServerName}</fpl:sServerName>
                         <!--Optional:-->
                         <fpl:sPointName>{PointName}</fpl:sPointName>
                         <!--Optional:-->
                         <fpl:sTimeStamp>{startTime}</fpl:sTimeStamp>
                      </fpl:GetArchiveValue>
                   </soapenv:Body>
                </soapenv:Envelope>
                """,
                'GetInterpolatedValues': """<?xml version="1.0" encoding="utf-8"?>
                    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:fpl="http://fpl.pgen/">
                    <soapenv:Header/>
                    <soapenv:Body>
                      <fpl:GetInterpolatedValues>
                         <!--Optional:-->
                         <fpl:serverName>{ServerName}</fpl:serverName>
                         <!--Optional:-->
                         <fpl:pointName>{PointName}</fpl:pointName>
                         <!--Optional:-->
                         <fpl:startTime>{startTime}</fpl:startTime>
                         <!--Optional:-->
                         <fpl:endTime>{endTime}</fpl:endTime>
                         <fpl:intervals>{Interval}</fpl:intervals>
                      </fpl:GetInterpolatedValues>
                   </soapenv:Body>
                </soapenv:Envelope>
                """
        }

        # Make request to the API using the SOAP pattern defined above:
        if rec['PI_METHOD'] == 'GetArchiveValue':
            body = rtr_pi_method_config['GetArchiveValue'].format(ServerName=rec['SERVER_NAME'],
                                                                  PointName=rec['TAG_NAME'],
                                                                  startTime=dts)
        elif rec['PI_METHOD'] == 'GetInterpolatedValues':
            body = rtr_pi_method_config['GetInterpolatedValues'].format(ServerName=rec['SERVER_NAME'],
                                                                 PointName=rec['TAG_NAME'],
                                                                 startTime=rec['DateStr'],
                                                                 endTime=rec['END_TIME'],
                                                                 Interval=rec['INTERVAL'])

        response = requests.post(url,data=body,headers=headers,proxies={
            "http": "http://proxy.com:8080",
            "https": "https://proxy.com:8080"})

        # Get needed values from repsonse:
        if response.status_code == 200:
            xml = response.content
            soup = BeautifulSoup(xml, 'lxml')
            return {
                'PLANT_ID': rec['PLANT_ID'],
                'TIMESTAMPLOCAL': dts,
                'TAG_TYPE': rec['TAG_TYPE'],
                'TAG_NAME': rec['TAG_NAME'],
                'VALUE': soup.body.value.text
            }


    def run(self):

        # Hold timeout times for logging: (Build feature to pull these later)
        time_losses = []

        # Avro settings:
        SCHEMA = avro.schema.Parse(json.dumps({
 		"namespace"    : "esbi_stg1.avro",
 		"type"         : "record",
 		"name"         : "ESBI_STG1_Data",
 		"fields"       : [
     			{"name": "plant_id", "type": "int"},
     			{"name": "timestamplocal", "type": "string"},
                        {"name": "tag_type", "type": "string"},
                        {"name": "tag_name", "type": "string"},
     			{"name": "value"  , "type": ["string", "null"]}
 		]
	}))

        #buffer = io.BytesIO()
        #writer = avro.io.DatumWriter(SCHEMA)
        #encoder = avro.io.BinaryEncoder(buffer)

        # Setup Producer:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            # Encode all values as JSON
            value_serializer=lambda value: json.dumps(value).encode(),
        )

        # while not self.stop_event.is_set():
	# Get current timestamp:
        curr_dts = datetime.now(timezone('US/Eastern')).replace(second=0, microsecond=0)
        step_dts = curr_dts - timedelta(minutes=1)
        step_dts = step_dts.astimezone(timezone('US/Eastern'))
        step_dts_str = step_dts.strftime('%Y-%m-%d %H:%M:00')

        while True:
           curr_dts = datetime.now(timezone('US/Eastern')).replace(second=0, microsecond=0)
           # Check the local timestamp setting:
           if curr_dts > step_dts:
               time_diff = (curr_dts - step_dts)
           else:
               time_diff = (step_dts - curr_dts)

           min_diff = (time_diff.seconds / 60)

           # Success: Difference betwen current and step timestamp is 1 minute:
           if min_diff == 1:

               for index, rec in self.config_data_df.iterrows():
                   try:
                       event: dict = self.call_api(rec, step_dts_str)
                       # writer.write(event, encoder)
                       # raw_bytes = buffer.getvalue()
                       producer.send(DATA_TOPIC, value=event)
                       print(event)
                   except:
                       pass

               # Get the current timestamp of actual time:
               curr_dts = datetime.now(timezone('US/Eastern')).replace(second=0, microsecond=0)
               # Update timestamps to determine next step of processing ONLY iff current ts processed:
               # Get next minute we are supposed to process:
               step_dts = step_dts + timedelta(minutes=1)
               step_dts_str = step_dts.strftime('%Y-%m-%d %H:%M:00')

           # FAILURE: Difference between step and current dts != 1
           else:
               # check if step timestamp is more than 1 minute away from current timestamp:
               if min_diff > 1:
                   base_time = min(step_dts, cur_dts)
                   # Need to append a range of minutes lost:
                   time_losses.append([base + datetime.timedelta(minutes=min) for min in range(0, min_diff)])
                   curr_dts = datetime.now(timezone('US/Eastern')).replace(second=0, microsecond=0)
                   # Skip step to the current timestamp:
                   step_dts = curr_dts - timedelta(minutes=1)
                   step_dts_str = step_dts.strftime('%Y-%m-%d %H:%M:00')
               else:
                   sleep(5)


        producer.close()
