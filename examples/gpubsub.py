#!/usr/bin/env python3

"""
Notice! This requires: google-cloud-pubsub==0.35.4
"""

import datetime
import json
import logging
import os
import random

from google.cloud import pubsub


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


TOPIC = 'projects/europython18/topics/ep18-topic'
SUBSCRIPTION = 'projects/europython18/subscriptions/ep18-sub'
PROJECT = 'europython18'


def get_publisher():
    client = pubsub.PublisherClient()
    try:
        client.create_topic(TOPIC)
    except Exception as e:
        # already created
        pass

    return client


def get_subscriber():
    client = pubsub.SubscriberClient()
    try:
        client.create_subscription(SUBSCRIPTION, TOPIC)
    except Exception:
        # already created
        pass
    return client


def produce_messages():
    publisher = get_publisher()
    futures = []
    for msg in range(1, 6):
        msg_data = {'msg_id': msg}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.info(f'Publishing {msg_data["msg_id"]}')


def consume_messages():
    client = get_subscriber()
    def callback(msg):
        print('xxx')
        msg.ack()
        data = json.loads(msg.data.decode('utf-8'))
        logging.info(f'Consumed {data["msg_id"]}')

    future = client.subscribe(SUBSCRIPTION, callback)
    print(future)
    # future = client.subscribe(SUBSCRIPTION, callback)

    # try:
    #     future.result()
    # except Exception as e:
    #     logging.error(f'Caught exception: {e}')
    #     client.close()


if __name__ == '__main__':
    # safety net, wouldn't want to do anything in prod
    assert os.environ.get('PUBSUB_EMULATOR_HOST'), 'You should be running the emulator'
    produce_messages()
    consume_messages()

