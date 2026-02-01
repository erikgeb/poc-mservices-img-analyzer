import json
import os
import uuid
import threading
from datetime import datetime, timezone

import cv2
import numpy as np
import pika
from PIL import Image, ImageDraw, ImageFont
from neo4j import GraphDatabase

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'

neo4j_driver = GraphDatabase.driver(
    os.environ['NEO4J_URI'],
    auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD'])
)

# Fan-in state: {workflowId: {metadata: ..., detections: ...}}
state = {}
state_lock = threading.Lock()


def record_event(event, prev_event_types):
    with neo4j_driver.session() as session:
        session.run(
            "MERGE (w:Workflow {id: $wid}) "
            "CREATE (e:Event {id: $eid, type: $type, timestamp: $ts}) "
            "CREATE (e)-[:BELONGS_TO]->(w)",
            wid=event['workflowId'], eid=event['eventId'],
            type=event['eventType'], ts=event['timestamp']
        )
        for prev_type in prev_event_types:
            session.run(
                "MATCH (w:Workflow {id: $wid})<-[:BELONGS_TO]-(prev:Event {type: $prevType}) "
                "MATCH (cur:Event {id: $curId}) "
                "CREATE (prev)-[:TRIGGERS]->(cur)",
                wid=event['workflowId'], prevType=prev_type, curId=event['eventId']
            )


def annotate(workflow_id, metadata, detections, filename):
    filepath = os.path.join(IMAGES_DIR, filename)
    img = cv2.imread(filepath)
    if img is None:
        print(f"Cannot read image {filepath}")
        return None

    # Draw bounding boxes
    for det in detections:
        x1, y1, x2, y2 = [int(v) for v in det['bbox']]
        cv2.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2)
        label = f"{det['label']} {det['confidence']:.2f}"
        cv2.putText(img, label, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

    # EXIF summary overlay (top-left)
    exif = metadata.get('exif', {})
    summary_lines = []
    for key in ['ImageWidth', 'ImageHeight', 'Format', 'Image Make', 'Image Model']:
        if key in exif:
            summary_lines.append(f"{key}: {exif[key]}")
    if not summary_lines:
        summary_lines.append("No EXIF data")

    y_offset = 20
    for line in summary_lines:
        cv2.putText(img, line, (10, y_offset), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
        y_offset += 20

    out_filename = f"{workflow_id}_annotated.jpg"
    out_path = os.path.join(IMAGES_DIR, out_filename)
    cv2.imwrite(out_path, img)
    return out_filename


def try_annotate(ch, workflow_id):
    with state_lock:
        entry = state.get(workflow_id, {})
        if 'metadata' not in entry or 'detections' not in entry:
            return
        metadata = entry['metadata']
        detections = entry['detections']
        filename = entry['filename']
        del state[workflow_id]

    print(f"Both events received for {workflow_id}, annotating...")
    out_filename = annotate(workflow_id, metadata, detections, filename)
    if not out_filename:
        return

    out_event = {
        'eventId': str(uuid.uuid4()),
        'eventType': 'image.annotated',
        'workflowId': workflow_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': {'filename': out_filename},
    }
    ch.basic_publish(exchange=EXCHANGE, routing_key='image.annotated',
                     body=json.dumps(out_event))
    record_event(out_event, ['image.metadata_extracted', 'image.objects_detected'])
    print(f"Annotated image saved: {out_filename}")


def on_metadata(ch, method, properties, body):
    event = json.loads(body)
    wid = event['workflowId']
    with state_lock:
        state.setdefault(wid, {})
        state[wid]['metadata'] = event['payload'].get('metadata', {})
        state[wid].setdefault('filename', event['payload']['filename'])
    try_annotate(ch, wid)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_detections(ch, method, properties, body):
    event = json.loads(body)
    wid = event['workflowId']
    with state_lock:
        state.setdefault(wid, {})
        state[wid]['detections'] = event['payload'].get('detections', [])
        state[wid].setdefault('filename', event['payload']['filename'])
    try_annotate(ch, wid)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    params = pika.URLParameters(os.environ['RABBITMQ_URL'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)

    channel.queue_declare(queue='annotator-metadata', durable=True)
    channel.queue_bind(queue='annotator-metadata', exchange=EXCHANGE, routing_key='image.metadata_extracted')

    channel.queue_declare(queue='annotator-detections', durable=True)
    channel.queue_bind(queue='annotator-detections', exchange=EXCHANGE, routing_key='image.objects_detected')

    channel.basic_consume(queue='annotator-metadata', on_message_callback=on_metadata)
    channel.basic_consume(queue='annotator-detections', on_message_callback=on_detections)

    print('Image Annotator waiting for messages...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
