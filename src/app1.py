import os
import json
import logging
import boto3
import botocore
from io import BytesIO
from urllib.parse import unquote_plus

try:
    from PIL import Image, ExifTags
except Exception:
    Image = None
    ExifTags = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_sqs_client():
    return boto3.client('sqs')


def _get_s3_client():
    return boto3.client('s3')


def _resolve_queue_url(sqs_client):
    # Prefer explicit URL via env var, fall back to QueueName lookup
    url = os.environ.get('METADATA_QUEUE_URL') or os.environ.get('SQS_QUEUE_URL')
    if url:
        return url
    name = os.environ.get('METADATA_QUEUE_NAME') or os.environ.get('SQS_QUEUE_NAME')
    if name:
        resp = sqs_client.get_queue_url(QueueName=name)
        return resp['QueueUrl']
    raise RuntimeError('No SQS queue URL or name provided in environment variables')


def ingest_handler(event, context):
    """
    Lambda handler triggered by S3 `ObjectCreated` events.
    For each created object, sends an SQS message with bucket and key.
    Expects environment variable `METADATA_QUEUE_URL` or `METADATA_QUEUE_NAME`.
    """
    logger.info('Received S3 event: %s', json.dumps(event))
    sqs = _get_sqs_client()
    try:
        queue_url = _resolve_queue_url(sqs)
    except Exception as e:
        logger.exception('Unable to resolve SQS queue URL: %s', e)
        raise

    records = event.get('Records', [])
    sent = 0
    for rec in records:
        s3 = rec.get('s3') or {}
        bucket = s3.get('bucket', {}).get('name')
        key = s3.get('object', {}).get('key')
        if not bucket or not key:
            logger.warning('Skipping record without bucket/key: %s', rec)
            continue
        # keys in S3 events are URL-encoded
        key = unquote_plus(key)

        message = {'bucket': bucket, 'key': key}
        try:
            resp = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
            logger.info('Sent SQS message for %s/%s result=%s', bucket, key, resp.get('MessageId'))
            sent += 1
        except Exception:
            logger.exception('Failed to send SQS message for %s/%s', bucket, key)

    return {'statusCode': 200, 'body': json.dumps({'messages_sent': sent})}


def _extract_exif(image):
    if not hasattr(image, '_getexif') or ExifTags is None:
        return {}
    exif_raw = image._getexif() or {}
    exif = {}
    for tag, value in exif_raw.items():
        name = ExifTags.TAGS.get(tag, tag)
        try:
            # Try to ensure json serializable values
            json.dumps(value)
            exif[name] = value
        except Exception:
            exif[name] = str(value)
    return exif


def metadata_handler(event, context):
    """
    Lambda handler triggered by SQS messages. Each message body should be JSON
    with `bucket` and `key` pointing to the image under `incoming/`.

    The function downloads the image, extracts metadata (format, width, height,
    filesize, EXIF), and writes a JSON file to the `metadata/` prefix in the same bucket.
    """
    logger.info('Received SQS event: %s', json.dumps(event))
    s3 = _get_s3_client()
    records = event.get('Records', [])
    processed = 0
    for rec in records:
        body = rec.get('body')
        if not body:
            logger.warning('SQS record missing body: %s', rec)
            continue
        try:
            msg = json.loads(body)
        except Exception:
            logger.exception('Failed to parse SQS body as JSON: %s', body)
            continue

        bucket = msg.get('bucket')
        key = msg.get('key')
        if not bucket or not key:
            logger.warning('Message missing bucket/key: %s', msg)
            continue

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()
            filesize = len(data)
            source_etag = obj.get('ETag', '').strip('"')
        except botocore.exceptions.ClientError:
            logger.exception('Failed to download %s/%s', bucket, key)
            continue

        meta = {'source_bucket': bucket, 'source_key': key, 'file_size_bytes': filesize}

        if Image is None:
            logger.warning('Pillow not installed; skipping image parsing')
            meta.update({'error': 'Pillow not available for image parsing'})
        else:
            try:
                img = Image.open(BytesIO(data))
                meta['format'] = img.format
                meta['width'], meta['height'] = img.size
                # EXIF extraction
                exif = _extract_exif(img)
                if exif:
                    meta['exif'] = exif
                img.close()
            except Exception:
                logger.exception('Failed to parse image %s/%s', bucket, key)

        # Create metadata key under metadata/ prefix, preserving base name and extension
        base = os.path.basename(key)
        #name, _ = os.path.splitext(base)
       
        metadata_key = f'metadata/{base}.json'
        
        # Idempotency check: if metadata exists and source ETag matches, skip writing
        try:
            existing_meta_obj = s3.get_object(Bucket=bucket, Key=metadata_key)
            existing_meta = json.loads(existing_meta_obj['Body'].read())
            if existing_meta.get('source_etag') == source_etag:
                logger.info('Metadata already exists with matching source ETag; skipping write for %s/%s', bucket, key)
                processed += 1
                continue
        except botocore.exceptions.ClientError:
            # Metadata doesn't exist yet, proceed with write
            pass
        
        # Add source ETag to metadata for idempotency tracking
        meta['source_etag'] = source_etag
        
        try:
            s3.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(meta).encode('utf-8'), ContentType='application/json')
            logger.info('Wrote metadata to s3://%s/%s', bucket, metadata_key)
            processed += 1
        except Exception:
            logger.exception('Failed to write metadata for %s/%s', bucket, key)

    return {'statusCode': 200, 'body': json.dumps({'processed': processed})}
