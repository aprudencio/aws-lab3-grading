from app1 import ingest_handler


def handler(event, context):
    return ingest_handler(event, context)
