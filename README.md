# aws-lab3-grading
The following architecture is deployed using this project:
S3 → Lambda (Ingest) → SQS → Lambda (Metadata Extractor) → S3

Where the first lambda will ingest an image, set the SQS so the second Lambda is triggered to extract metadata from the image and store that data in JSON on an S3 bucket