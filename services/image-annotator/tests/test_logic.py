import json
import threading
from unittest.mock import MagicMock, patch
from PIL import Image

from logic import (
    annotate, update_state, try_annotate, on_metadata, on_detections,
    load_font, draw_text_with_bg, record_event,
)


def _create_test_image(tmp_path, filename='wf-1.jpg', size=(100, 100)):
    img = Image.new('RGB', size, color='red')
    path = tmp_path / filename
    img.save(str(path), 'JPEG')
    return str(path)


class TestAnnotate:
    def test_produces_annotated_file(self, tmp_path):
        _create_test_image(tmp_path, 'wf-1.jpg')
        detections = [{'label': 'cat', 'confidence': 0.95, 'bbox': [10, 10, 50, 50]}]
        metadata = {'exif': {'ImageWidth': 100, 'ImageHeight': 100}}

        result = annotate('wf-1', metadata, detections, 'wf-1.jpg', images_dir=str(tmp_path))
        assert result == 'wf-1_annotated.jpg'
        assert (tmp_path / 'wf-1_annotated.jpg').exists()

    def test_draws_no_exif_label(self, tmp_path):
        _create_test_image(tmp_path, 'wf-1.jpg')
        result = annotate('wf-1', {}, [], 'wf-1.jpg', images_dir=str(tmp_path))
        assert result == 'wf-1_annotated.jpg'

    def test_returns_none_for_missing_file(self, tmp_path):
        result = annotate('wf-1', {}, [], 'missing.jpg', images_dir=str(tmp_path))
        assert result is None


class TestUpdateState:
    def test_sets_metadata(self):
        state = {}
        lock = threading.Lock()
        update_state(state, lock, 'wf-1', 'metadata', {'exif': {}}, 'wf-1.jpg')
        assert state['wf-1']['metadata'] == {'exif': {}}
        assert state['wf-1']['filename'] == 'wf-1.jpg'

    def test_sets_detections(self):
        state = {}
        lock = threading.Lock()
        update_state(state, lock, 'wf-1', 'detections', [{'label': 'cat'}], 'wf-1.jpg')
        assert state['wf-1']['detections'] == [{'label': 'cat'}]


class TestTryAnnotate:
    def test_returns_none_when_incomplete(self):
        state = {'wf-1': {'metadata': {}}}
        lock = threading.Lock()
        ch = MagicMock()
        driver = MagicMock()
        result = try_annotate(ch, 'wf-1', state, lock, driver)
        assert result is None
        assert 'wf-1' in state  # not cleaned up

    def test_annotates_when_both_present(self, tmp_path):
        _create_test_image(tmp_path, 'wf-1.jpg')
        state = {
            'wf-1': {
                'metadata': {'exif': {}},
                'detections': [],
                'filename': 'wf-1.jpg',
            }
        }
        lock = threading.Lock()
        ch = MagicMock()

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = try_annotate(ch, 'wf-1', state, lock, driver, images_dir=str(tmp_path))
        assert result is not None
        assert result['eventType'] == 'image.annotated'
        assert 'wf-1' not in state  # cleaned up
        ch.basic_publish.assert_called_once()

    def test_cleans_up_state(self, tmp_path):
        _create_test_image(tmp_path, 'wf-1.jpg')
        state = {
            'wf-1': {
                'metadata': {'exif': {}},
                'detections': [],
                'filename': 'wf-1.jpg',
            }
        }
        lock = threading.Lock()
        ch = MagicMock()
        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        try_annotate(ch, 'wf-1', state, lock, driver, images_dir=str(tmp_path))
        assert 'wf-1' not in state


class TestOnMetadata:
    def test_metadata_first_does_not_annotate(self):
        state = {}
        lock = threading.Lock()
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'
        driver = MagicMock()

        body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg', 'metadata': {'exif': {}}},
        }).encode()

        result = on_metadata(ch, method, body, state, lock, driver)
        assert result is None
        ch.basic_ack.assert_called_once()

    def test_detections_first_does_not_annotate(self):
        state = {}
        lock = threading.Lock()
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'
        driver = MagicMock()

        body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg', 'detections': []},
        }).encode()

        result = on_detections(ch, method, body, state, lock, driver)
        assert result is None
        ch.basic_ack.assert_called_once()


class TestOnDetections:
    def test_both_arrive_triggers_annotation(self, tmp_path):
        _create_test_image(tmp_path, 'wf-1.jpg')
        state = {}
        lock = threading.Lock()
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # First: metadata
        meta_body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg', 'metadata': {'exif': {}}},
        }).encode()
        on_metadata(ch, method, meta_body, state, lock, driver, images_dir=str(tmp_path))

        # Second: detections — should trigger annotation
        det_body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg', 'detections': []},
        }).encode()
        result = on_detections(ch, method, det_body, state, lock, driver, images_dir=str(tmp_path))
        assert result is not None
        assert result['eventType'] == 'image.annotated'


class TestLoadFont:
    def test_returns_font(self):
        font = load_font(16)
        assert font is not None


class TestRecordEvent:
    def test_records_with_multiple_triggers(self):
        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        event = {'workflowId': 'wf-1', 'eventId': 'e-1', 'eventType': 'image.annotated', 'timestamp': 't'}
        record_event(driver, event, ['image.metadata_extracted', 'image.objects_detected'])
        assert session.run.call_count == 3  # 1 create + 2 triggers
