"""
From https://github.com/cloudfind/google-pubsub-asyncio/blob/master/app.py

Example implementation of asyncio with Google's custom asynchronous PubSub client.
"""
import asyncio
import datetime
import functools
import os

from google.cloud import pubsub_v1 as pubsub
from google.gax.errors import RetryError
from grpc import StatusCode

PROJECT = os.environ['GOOGLE_CLOUD_PROJECT']
TOPIC = os.environ['TOPIC']
SUBSCRIPTION = TOPIC + ".subscription"
assert os.environ['PUBSUB_EMULATOR_HOST']
print(PROJECT)

async def message_producer(publisher):
    """ Publish messages which consist of the current datetime """
    while True:
        publisher(bytes(str(datetime.datetime.now()), "utf8"))
        await asyncio.sleep(0.1)


async def print_message(message):
    """ Do something asynchronous, print the message and ack it """
    await asyncio.sleep(0.1)
    print(message.data.decode())
    message.ack()


def main():
    """ Main program """
    loop = asyncio.get_event_loop()

    topic = "projects/{project_id}/topics/{topic}".format(
        project_id=PROJECT, topic=TOPIC)
    subscription_name = "projects/{project_id}/subscriptions/{subscription}".format(
        project_id=PROJECT, subscription=SUBSCRIPTION)

    publisher, subscription = make_publisher_subscription(
        topic, subscription_name)

    def create_print_message_task(message):
        """ Callback handler for the subscription; schedule a task on the event loop """
        loop.create_task(print_message(message))

    subscription.open(create_print_message_task)

    # Produce some messages to consume
    loop.create_task(message_producer(
        functools.partial(publisher.publish, topic)))

    loop.run_forever()


def make_publisher_subscription(topic, subscription_name):
    """ Make a publisher and subscriber client, and create the necessary resources """
    publisher = pubsub.PublisherClient()
    try:p
        publisher.create_topic(topic)
        print('xxx')
    except RetryError as exc:
        if exc.cause.code() is not StatusCode.ALREADY_EXISTS:
            raise
    print('publisher created')
    subscriber = pubsub.SubscriberClient()
    try:
        subscriber.create_subscription(subscription_name, topic)
    except RetryError as exc:
        if exc.cause.code() is not StatusCode.ALREADY_EXISTS:
            raise
    print('subscription created')
    subscription = subscriber.subscribe(subscription_name)

    return publisher, subscription


if __name__ == "__main__":
    main()
