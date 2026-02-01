import os
import threading

import pika
from neo4j import GraphDatabase

from logic import EXCHANGE, on_metadata, on_detections

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)

state = {}
state_lock = threading.Lock()


def _on_metadata(ch, method, properties, body):
    on_metadata(ch, method, body, state, state_lock, neo4j_driver)


def _on_detections(ch, method, properties, body):
    on_detections(ch, method, body, state, state_lock, neo4j_driver)


def main():
    params = pika.URLParameters(os.environ['RABBITMQ_URL'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)

    channel.queue_declare(queue='annotator-metadata', durable=True)
    channel.queue_bind(queue='annotator-metadata', exchange=EXCHANGE, routing_key='image.metadata_extracted')

    channel.queue_declare(queue='annotator-detections', durable=True)
    channel.queue_bind(queue='annotator-detections', exchange=EXCHANGE, routing_key='image.objects_detected')

    channel.basic_consume(queue='annotator-metadata', on_message_callback=_on_metadata)
    channel.basic_consume(queue='annotator-detections', on_message_callback=_on_detections)

    print('Image Annotator waiting for messages...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
