import json
import os
import uuid
import threading
from datetime import datetime, timezone

from PIL import Image, ImageDraw, ImageFont

EXCHANGE = 'imageanalyzer.events'
IMAGES_DIR = '/data/images'


def record_event(neo4j_driver, event, prev_event_types):
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


def load_font(size):
    for path in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def draw_text_with_bg(draw, xy, text, font, fill=(255, 255, 255), bg=(0, 0, 0, 160), padding=4):
    bbox = draw.textbbox(xy, text, font=font)
    draw.rectangle(
        [bbox[0] - padding, bbox[1] - padding, bbox[2] + padding, bbox[3] + padding],
        fill=bg,
    )
    draw.text(xy, text, font=font, fill=fill)


def annotate(workflow_id, metadata, detections, filename, images_dir=None):
    if images_dir is None:
        images_dir = IMAGES_DIR
    filepath = os.path.join(images_dir, filename)
    try:
        img = Image.open(filepath).convert('RGBA')
    except Exception:
        return None

    overlay = Image.new('RGBA', img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    label_font = load_font(16)
    exif_font = load_font(14)

    for det in detections:
        x1, y1, x2, y2 = [int(v) for v in det['bbox']]
        draw.rectangle([x1, y1, x2, y2], outline=(0, 255, 0, 255), width=2)
        label = f"{det['label']} {det['confidence']:.2f}"
        draw_text_with_bg(draw, (x1, y1 - 22), label, label_font,
                          fill=(255, 255, 255, 255), bg=(0, 180, 0, 180))

    exif = metadata.get('exif', {})
    summary_lines = []
    for key in ['ImageWidth', 'ImageHeight', 'Format', 'Image Make', 'Image Model']:
        if key in exif:
            summary_lines.append(f"{key}: {exif[key]}")
    if not summary_lines:
        summary_lines.append("No EXIF data")

    y_offset = 10
    for line in summary_lines:
        draw_text_with_bg(draw, (10, y_offset), line, exif_font,
                          fill=(255, 255, 255, 255), bg=(0, 0, 0, 160))
        y_offset += 24

    img = Image.alpha_composite(img, overlay).convert('RGB')
    out_filename = f"{workflow_id}_annotated.jpg"
    out_path = os.path.join(images_dir, out_filename)
    img.save(out_path, 'JPEG', quality=92)
    return out_filename


def update_state(state, state_lock, workflow_id, key, value, filename):
    with state_lock:
        state.setdefault(workflow_id, {})
        state[workflow_id][key] = value
        state[workflow_id].setdefault('filename', filename)


def try_annotate(ch, workflow_id, state, state_lock, neo4j_driver, images_dir=None):
    with state_lock:
        entry = state.get(workflow_id, {})
        if 'metadata' not in entry or 'detections' not in entry:
            return None
        metadata = entry['metadata']
        detections = entry['detections']
        filename = entry['filename']
        del state[workflow_id]

    out_filename = annotate(workflow_id, metadata, detections, filename, images_dir)
    if not out_filename:
        return None

    out_event = {
        'eventId': str(uuid.uuid4()),
        'eventType': 'image.annotated',
        'workflowId': workflow_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': {'filename': out_filename},
    }
    ch.basic_publish(exchange=EXCHANGE, routing_key='image.annotated',
                     body=json.dumps(out_event))
    record_event(neo4j_driver, out_event, ['image.metadata_extracted', 'image.objects_detected'])
    return out_event


def on_metadata(ch, method, body, state, state_lock, neo4j_driver, images_dir=None):
    event = json.loads(body)
    wid = event['workflowId']
    update_state(state, state_lock, wid, 'metadata', event['payload'].get('metadata', {}), event['payload']['filename'])
    result = try_annotate(ch, wid, state, state_lock, neo4j_driver, images_dir)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return result


def on_detections(ch, method, body, state, state_lock, neo4j_driver, images_dir=None):
    event = json.loads(body)
    wid = event['workflowId']
    update_state(state, state_lock, wid, 'detections', event['payload'].get('detections', []), event['payload']['filename'])
    result = try_annotate(ch, wid, state, state_lock, neo4j_driver, images_dir)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return result
