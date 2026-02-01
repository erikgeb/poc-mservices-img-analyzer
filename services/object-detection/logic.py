import json
import os
import uuid
from datetime import datetime, timezone

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'
MAX_DETECTIONS = 20


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


def record_entities(neo4j_driver, workflow_id, detections):
    if not detections:
        return
    with neo4j_driver.session() as session:
        for det in detections:
            session.run(
                "MERGE (e:Entity {label: $label}) "
                "WITH e "
                "MATCH (w:Workflow {id: $wid}) "
                "CREATE (w)-[:DETECTED {confidence: $conf, bbox: $bbox}]->(e)",
                label=det['label'], wid=workflow_id,
                conf=det['confidence'], bbox=det['bbox']
            )


def detect_objects(filepath, model):
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


def handle_message(ch, method, body, neo4j_driver, model, images_dir=None):
    if images_dir is None:
        images_dir = IMAGES_DIR
    event = json.loads(body)
    workflow_id = event['workflowId']
    filename = event['payload']['filename']
    filepath = os.path.join(images_dir, filename)

    detections = detect_objects(filepath, model)

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
    record_event(neo4j_driver, out_event, 'image.fetched')
    record_entities(neo4j_driver, workflow_id, detections)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return out_event
