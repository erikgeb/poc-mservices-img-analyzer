import json
import os
import uuid
from datetime import datetime, timezone

import pika
from neo4j import GraphDatabase
from ultralytics import YOLO

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'
MAX_DETECTIONS = 20

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)

model = YOLO('yolov8n.pt')


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


def detect_objects(filepath):
    results = model(filepath, verbose=False)
    detections = []
    for r in results:
        for box in r.boxes:
            if len(detections) >= MAX_DETECTIONS:
                break
            detections.append({
                'label': model.names[int(box.cls[0])],
                'confidence': round(float(box.conf[0]), 3),
                'bbox': [round(float(x), 1) for x in box.xyxy[0].tolist()],
            })
    return detections


def on_message(ch, method, properties, body):
    event = json.loads(body)
    workflow_id = event['workflowId']
    filename = event['payload']['filename']
    filepath = os.path.join(IMAGES_DIR, filename)
    print(f"Detecting objects in {filename}")

    detections = detect_objects(filepath)

    out_event = {
        'eventId': str(uuid.uuid4()),
        'eventType': 'image.objects_detected',
        'workflowId': workflow_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': {
            'filename': filename,
            'detections': detections,
        },
    }
    ch.basic_publish(exchange=EXCHANGE, routing_key='image.objects_detected',
                     body=json.dumps(out_event))
    record_event(out_event, 'image.fetched')
    print(f"Detected {len(detections)} objects in {filename}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


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
