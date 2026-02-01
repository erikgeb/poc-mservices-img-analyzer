import os

import pika
from neo4j import GraphDatabase
from ultralytics import YOLO

from logic import EXCHANGE, handle_message

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)

model = YOLO('yolov8n.pt')


def on_message(ch, method, properties, body):
    handle_message(ch, method, body, neo4j_driver, model)


def main():
    params = pika.URLParameters(os.environ['RABBITMQ_URL'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
    channel.queue_declare(queue='object-detection', durable=True)
    channel.queue_bind(queue='object-detection', exchange=EXCHANGE, routing_key='image.fetched')
    channel.basic_consume(queue='object-detection', on_message_callback=on_message)
    print('Object Detection waiting for messages...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
