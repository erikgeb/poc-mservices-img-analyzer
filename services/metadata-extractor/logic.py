import json
import os
import uuid
from datetime import datetime, timezone

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'


def record_event(neo4j_driver, event, prev_event_type):
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


def extract_metadata(filepath, exifread_module, pil_image_class):
    exif = {}
    try:
        with open(filepath, 'rb') as f:
            tags = exifread_module.process_file(f, details=False)
            for k, v in tags.items():
                exif[k] = str(v)
    except Exception:
        pass

    try:
        img = pil_image_class.open(filepath)
        exif['ImageWidth'] = img.width
        exif['ImageHeight'] = img.height
        exif['Format'] = img.format or 'JPEG'
    except Exception:
        pass

    return exif


def handle_message(ch, method, body, neo4j_driver, exifread_module, pil_image_class, images_dir=None):
    if images_dir is None:
        images_dir = IMAGES_DIR
    event = json.loads(body)
    workflow_id = event['workflowId']
    filename = event['payload']['filename']
    filepath = os.path.join(images_dir, filename)

    metadata = extract_metadata(filepath, exifread_module, pil_image_class)

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
    record_event(neo4j_driver, out_event, 'image.fetched')
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return out_event
