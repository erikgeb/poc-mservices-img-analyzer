import os

import pika
import exifread
from PIL import Image
from neo4j import GraphDatabase

from logic import EXCHANGE, handle_message

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)


def on_message(ch, method, properties, body):
    handle_message(ch, method, body, neo4j_driver, exifread, Image)


def main():
    params = pika.URLParameters(os.environ['RABBITMQ_URL'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
    channel.queue_declare(queue='metadata-extractor', durable=True)
    channel.queue_bind(queue='metadata-extractor', exchange=EXCHANGE, routing_key='image.fetched')
    channel.basic_consume(queue='metadata-extractor', on_message_callback=on_message)
    print('Metadata Extractor waiting for messages...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
