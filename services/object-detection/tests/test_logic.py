import json
from unittest.mock import MagicMock

from logic import detect_objects, record_event, handle_message, MAX_DETECTIONS


def _make_box(cls_id, conf, bbox):
    box = MagicMock()
    box.cls = [cls_id]
    box.conf = [conf]
    box.xyxy = [MagicMock()]
    box.xyxy[0].tolist.return_value = bbox
    return box


class TestDetectObjects:
    def test_formats_detections(self):
        model = MagicMock()
        model.names = {0: 'cat', 1: 'dog'}
        result = MagicMock()
        result.boxes = [_make_box(0, 0.95123, [10.0, 20.0, 100.0, 200.0])]
        model.return_value = [result]

        detections = detect_objects('/fake/path.jpg', model)
        assert len(detections) == 1
        assert detections[0]['label'] == 'cat'
        assert detections[0]['confidence'] == 0.951
        assert detections[0]['bbox'] == [10.0, 20.0, 100.0, 200.0]

    def test_caps_at_max_detections(self):
        model = MagicMock()
        model.names = {0: 'cat'}
        boxes = [_make_box(0, 0.9, [0, 0, 10, 10]) for _ in range(30)]
        result = MagicMock()
        result.boxes = boxes
        model.return_value = [result]

        detections = detect_objects('/fake/path.jpg', model)
        assert len(detections) == MAX_DETECTIONS

    def test_empty_detections(self):
        model = MagicMock()
        model.names = {}
        result = MagicMock()
        result.boxes = []
        model.return_value = [result]

        detections = detect_objects('/fake/path.jpg', model)
        assert detections == []


class TestRecordEvent:
    def test_records_with_triggers(self):
        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        event = {'workflowId': 'wf-1', 'eventId': 'e-1', 'eventType': 'image.objects_detected', 'timestamp': 't'}
        record_event(driver, event, 'image.fetched')
        assert session.run.call_count == 2


class TestHandleMessage:
    def test_processes_message(self, tmp_path):
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'

        body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg'},
        }).encode()

        model = MagicMock()
        model.names = {0: 'cat'}
        result_obj = MagicMock()
        result_obj.boxes = [_make_box(0, 0.9, [10, 20, 100, 200])]
        model.return_value = [result_obj]

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = handle_message(ch, method, body, driver, model, images_dir=str(tmp_path))

        assert result['eventType'] == 'image.objects_detected'
        assert len(result['payload']['detections']) == 1
        ch.basic_publish.assert_called_once()
        ch.basic_ack.assert_called_once_with(delivery_tag='tag-1')

    def test_event_envelope_structure(self, tmp_path):
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'

        body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg'},
        }).encode()

        model = MagicMock()
        model.names = {}
        result_obj = MagicMock()
        result_obj.boxes = []
        model.return_value = [result_obj]

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = handle_message(ch, method, body, driver, model, images_dir=str(tmp_path))

        assert 'eventId' in result
        assert 'timestamp' in result
        assert result['payload']['filename'] == 'wf-1.jpg'
        assert result['payload']['detections'] == []
