import json
import os
import uuid
from datetime import datetime, timezone

import pika
import exifread
from PIL import Image
from neo4j import GraphDatabase

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)


def record_event(event, prev_event_type):
    with neo4j_driver.session() as session:
        session.run(
            "MERGE (w:Workflow {id: $wid}) "
            "CREATE (e:Event {id: $eid, type: $type, timestamp: $ts}) "
            "CREATE (e)-[:BELONGS_TO]->(w)",
            wid=event['workflowId'], eid=event['eventId'],
            type=event['eventType'], ts=event['timestamp']
        )
        if prev_event_type:
            session.run(
                "MATCH (w:Workflow {id: $wid})<-[:BELONGS_TO]-(prev:Event {type: $prevType}) "
                "MATCH (cur:Event {id: $curId}) "
                "CREATE (prev)-[:TRIGGERS]->(cur)",
                wid=event['workflowId'], prevType=prev_event_type, curId=event['eventId']
            )


def extract_metadata(filepath):
    exif = {}
    try:
        with open(filepath, 'rb') as f:
            tags = exifread.process_file(f, details=False)
            for k, v in tags.items():
                exif[k] = str(v)
    except Exception:
        pass

    try:
        img = Image.open(filepath)
        exif['ImageWidth'] = img.width
        exif['ImageHeight'] = img.height
        exif['Format'] = img.format or 'JPEG'
    except Exception:
        pass

    return exif


def on_message(ch, method, properties, body):
    event = json.loads(body)
    workflow_id = event['workflowId']
    filename = event['payload']['filename']
    filepath = os.path.join(IMAGES_DIR, filename)
    print(f"Extracting metadata for {filename}")

    metadata = extract_metadata(filepath)

    out_event = {
        'eventId': str(uuid.uuid4()),
        'eventType': 'image.metadata_extracted',
        'workflowId': workflow_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': {
            'filename': filename,
            'metadata': {'exif': metadata},
        },
    }
    ch.basic_publish(exchange=EXCHANGE, routing_key='image.metadata_extracted',
                     body=json.dumps(out_event))
    record_event(out_event, 'image.fetched')
    print(f"Metadata extracted for {filename}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


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
