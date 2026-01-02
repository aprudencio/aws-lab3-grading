"""Unit tests for Lambda handlers using pytest."""
import json
import os
import sys
import types
from io import BytesIO
from unittest.mock import patch, MagicMock

import pytest


# Setup fake boto3/botocore modules before importing app1
fake_boto3 = types.SimpleNamespace()
fake_boto3.client = MagicMock()
sys.modules['boto3'] = fake_boto3

fake_botocore = types.SimpleNamespace()
fake_botocore.exceptions = types.SimpleNamespace(ClientError=Exception)
sys.modules['botocore'] = fake_botocore

# Now import app1
import importlib.util
parent = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
src_path = os.path.join(parent, 'src')
spec = importlib.util.spec_from_file_location('app1', os.path.join(src_path, 'app1.py'))
app1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app1)


# Tiny 1x1 PNG for testing
PNG_1x1 = (
    b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01'
    b'\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01'
    b'\xe2!\xbc3\x00\x00\x00\x00IEND\xaeB`\x82'
)


class MockS3:
    """Mock S3 client for testing."""

    def __init__(self):
        self.store = {}
        self.get_calls = []
        self.put_calls = []

    def get_object(self, Bucket, Key):
        self.get_calls.append({'Bucket': Bucket, 'Key': Key})
        return {'Body': BytesIO(PNG_1x1)}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        if hasattr(Body, 'read'):
            content = Body.read()
        else:
            content = Body
        self.store[Key] = content
        self.put_calls.append({'Bucket': Bucket, 'Key': Key, 'ContentType': ContentType})
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}


class MockSQS:
    """Mock SQS client for testing."""

    def __init__(self):
        self.messages = []

    def send_message(self, QueueUrl, MessageBody):
        self.messages.append({'QueueUrl': QueueUrl, 'MessageBody': MessageBody})
        return {'MessageId': 'mock-msg-id-12345'}

    def get_queue_url(self, QueueName):
        return {'QueueUrl': f'https://queue.amazonaws.com/{QueueName}'}


@pytest.fixture
def mock_s3():
    return MockS3()


@pytest.fixture
def mock_sqs():
    return MockSQS()


@pytest.fixture
def aws_clients(mock_s3, mock_sqs):
    """Fixture to patch boto3 client creation."""
    def fake_client(name, *args, **kwargs):
        if name == 's3':
            return mock_s3
        if name == 'sqs':
            return mock_sqs
        raise RuntimeError(f'Unexpected boto3 client: {name}')

    with patch.object(app1.boto3, 'client', side_effect=fake_client):
        yield {'s3': mock_s3, 'sqs': mock_sqs}


class TestIngestHandler:
    """Tests for the ingest_handler function."""

    def test_ingest_handler_sends_sqs_message(self, aws_clients, monkeypatch):
        """Test that ingest_handler sends SQS message for each S3 object."""
        monkeypatch.setenv('METADATA_QUEUE_URL', 'mock-queue-url')

        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'incoming/test.png'},
                    }
                }
            ]
        }

        response = app1.ingest_handler(s3_event, None)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['messages_sent'] == 1

        mock_sqs = aws_clients['sqs']
        assert len(mock_sqs.messages) == 1
        assert mock_sqs.messages[0]['QueueUrl'] == 'mock-queue-url'

        msg_body = json.loads(mock_sqs.messages[0]['MessageBody'])
        assert msg_body['bucket'] == 'test-bucket'
        assert msg_body['key'] == 'incoming/test.png'

    def test_ingest_handler_multiple_records(self, aws_clients, monkeypatch):
        """Test ingest_handler with multiple S3 records."""
        monkeypatch.setenv('METADATA_QUEUE_URL', 'mock-queue-url')

        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'incoming/image1.png'},
                    }
                },
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'incoming/image2.jpg'},
                    }
                },
            ]
        }

        response = app1.ingest_handler(s3_event, None)
        body = json.loads(response['body'])

        assert body['messages_sent'] == 2
        mock_sqs = aws_clients['sqs']
        assert len(mock_sqs.messages) == 2

    def test_ingest_handler_missing_queue_url(self, aws_clients):
        """Test that ingest_handler raises error without METADATA_QUEUE_URL."""
        # Clear env var
        if 'METADATA_QUEUE_URL' in os.environ:
            del os.environ['METADATA_QUEUE_URL']
        if 'SQS_QUEUE_URL' in os.environ:
            del os.environ['SQS_QUEUE_URL']
        if 'METADATA_QUEUE_NAME' in os.environ:
            del os.environ['METADATA_QUEUE_NAME']
        if 'SQS_QUEUE_NAME' in os.environ:
            del os.environ['SQS_QUEUE_NAME']

        s3_event = {'Records': []}

        with pytest.raises(RuntimeError, match='No SQS queue URL or name'):
            app1.ingest_handler(s3_event, None)


class TestMetadataHandler:
    """Tests for the metadata_handler function."""

    def test_metadata_handler_extracts_and_stores(self, aws_clients):
        """Test that metadata_handler extracts image metadata and stores JSON."""
        sqs_event = {
            'Records': [
                {
                    'body': json.dumps({'bucket': 'test-bucket', 'key': 'incoming/test.png'})
                }
            ]
        }

        response = app1.metadata_handler(sqs_event, None)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['processed'] == 1

        mock_s3 = aws_clients['s3']
        assert len(mock_s3.get_calls) == 1
        assert mock_s3.get_calls[0]['Key'] == 'incoming/test.png'

        assert len(mock_s3.put_calls) == 1
        assert mock_s3.put_calls[0]['Key'] == 'metadata/test.png.json'
        assert mock_s3.put_calls[0]['ContentType'] == 'application/json'

    def test_metadata_handler_stores_correct_json(self, aws_clients):
        """Test that stored metadata JSON has expected fields."""
        sqs_event = {
            'Records': [
                {
                    'body': json.dumps({'bucket': 'test-bucket', 'key': 'incoming/photo.png'})
                }
            ]
        }

        app1.metadata_handler(sqs_event, None)

        mock_s3 = aws_clients['s3']
        stored_content = mock_s3.store['metadata/photo.json'].decode('utf-8')
        metadata = json.loads(stored_content)

        assert metadata['bucket'] == 'test-bucket'
        assert metadata['key'] == 'incoming/photo.png'
        assert 'file_size_bytes' in metadata
        assert metadata['file_size_bytes'] == len(PNG_1x1)

    def test_metadata_handler_multiple_messages(self, aws_clients):
        """Test metadata_handler with multiple SQS messages."""
        sqs_event = {
            'Records': [
                {'body': json.dumps({'bucket': 'test-bucket', 'key': 'incoming/img1.png'})},
                {'body': json.dumps({'bucket': 'test-bucket', 'key': 'incoming/img2.png'})},
            ]
        }

        response = app1.metadata_handler(sqs_event, None)
        body = json.loads(response['body'])

        assert body['processed'] == 2
        mock_s3 = aws_clients['s3']
        assert len(mock_s3.put_calls) == 2

    def test_metadata_handler_invalid_json(self, aws_clients):
        """Test metadata_handler gracefully handles invalid JSON in SQS message."""
        sqs_event = {'Records': [{'body': 'not-valid-json'}]}

        response = app1.metadata_handler(sqs_event, None)
        body = json.loads(response['body'])

        assert body['processed'] == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
