import json
from unittest.mock import MagicMock, patch, mock_open

from logic import extract_metadata, record_event, handle_message


class TestExtractMetadata:
    def test_with_exif_data(self, tmp_path):
        filepath = tmp_path / "test.jpg"
        filepath.write_bytes(b'\xff\xd8\xff')

        exifread_mod = MagicMock()
        exifread_mod.process_file.return_value = {'Image Make': 'Canon', 'Image Model': 'EOS'}

        pil_img = MagicMock()
        pil_img.open.return_value = MagicMock(width=800, height=600, format='JPEG')

        result = extract_metadata(str(filepath), exifread_mod, pil_img)
        assert result['Image Make'] == 'Canon'
        assert result['ImageWidth'] == 800
        assert result['ImageHeight'] == 600
        assert result['Format'] == 'JPEG'

    def test_without_exif_data(self, tmp_path):
        filepath = tmp_path / "test.jpg"
        filepath.write_bytes(b'\xff\xd8\xff')

        exifread_mod = MagicMock()
        exifread_mod.process_file.return_value = {}

        pil_img = MagicMock()
        pil_img.open.return_value = MagicMock(width=1024, height=768, format='PNG')

        result = extract_metadata(str(filepath), exifread_mod, pil_img)
        assert 'Image Make' not in result
        assert result['ImageWidth'] == 1024
        assert result['Format'] == 'PNG'

    def test_corrupt_file(self, tmp_path):
        filepath = tmp_path / "corrupt.jpg"
        filepath.write_bytes(b'not an image')

        exifread_mod = MagicMock()
        exifread_mod.process_file.side_effect = Exception("bad file")

        pil_img = MagicMock()
        pil_img.open.side_effect = Exception("cannot open")

        result = extract_metadata(str(filepath), exifread_mod, pil_img)
        assert result == {}


class TestRecordEvent:
    def test_records_event_with_triggers(self):
        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        event = {'workflowId': 'wf-1', 'eventId': 'e-1', 'eventType': 'image.metadata_extracted', 'timestamp': 't'}
        record_event(driver, event, 'image.fetched')
        assert session.run.call_count == 2

    def test_records_event_without_triggers(self):
        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        event = {'workflowId': 'wf-1', 'eventId': 'e-1', 'eventType': 'workflow.started', 'timestamp': 't'}
        record_event(driver, event, None)
        assert session.run.call_count == 1


class TestHandleMessage:
    def test_processes_message_end_to_end(self, tmp_path):
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 'tag-1'

        body = json.dumps({
            'workflowId': 'wf-1',
            'payload': {'filename': 'wf-1.jpg'},
        }).encode()

        filepath = tmp_path / "wf-1.jpg"
        filepath.write_bytes(b'\xff\xd8\xff')

        exifread_mod = MagicMock()
        exifread_mod.process_file.return_value = {}
        pil_img = MagicMock()
        pil_img.open.return_value = MagicMock(width=800, height=600, format='JPEG')

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = handle_message(ch, method, body, driver, exifread_mod, pil_img, images_dir=str(tmp_path))

        assert result['eventType'] == 'image.metadata_extracted'
        assert result['workflowId'] == 'wf-1'
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

        filepath = tmp_path / "wf-1.jpg"
        filepath.write_bytes(b'\xff\xd8\xff')

        exifread_mod = MagicMock()
        exifread_mod.process_file.return_value = {}
        pil_img = MagicMock()
        pil_img.open.return_value = MagicMock(width=800, height=600, format='JPEG')

        session = MagicMock()
        driver = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = handle_message(ch, method, body, driver, exifread_mod, pil_img, images_dir=str(tmp_path))

        assert 'eventId' in result
        assert 'timestamp' in result
        assert result['payload']['filename'] == 'wf-1.jpg'
        assert 'exif' in result['payload']['metadata']
