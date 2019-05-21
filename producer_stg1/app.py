"""Produce events into a Kafka topic."""
import threading, logging, time
import multiprocessing
import os
from time import sleep
import json

from kafka import KafkaProducer
from event import EventProducer

def main():

    tasks = [EventProducer('postgresql+psycopg2://postgres:postgres@db/es_dw?port=5432')]
    # tasks = [EventProducer('postgres://db:5432')]

    for t in tasks:
        t.start()

    # time.sleep(60)

    for task in tasks:
        task.stop()

    '''
    for task in tasks:
        task.join()
    '''

if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
