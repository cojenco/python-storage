from opentelemetry import trace
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.cloud_trace_propagator import (
    CloudTraceFormatPropagator,
)
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from google.cloud import storage

# [START]  Enable OTel Tracing
# by creating an exporter, creating a tracer provider, and registering that exporter.
set_global_textmap(CloudTraceFormatPropagator())
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(
    BatchSpanProcessor(CloudTraceSpanExporter())
)
trace.set_tracer_provider(tracer_provider)

# Optional yet recommended to instrument the requests HTTP library.
from opentelemetry.instrumentation.requests import RequestsInstrumentor
RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
# # [END] Enable OTel Tracing

# Metadata - Get Service Account
client = storage.Client()
email = client.get_service_account_email()
print(f"Succesfully got service account email {email}")

BUCKET_NAME="bucket2c"
BLOB_NAME = "prototrace212"
BLOB_NAME_2 = "new.json"
LOCAL_FILE_NAME = "/Users/cathyo/Downloads/nz.csv"
_RESUMABLE_UPLOAD_CHUNK_SIZE = 2 * 1024 * 1024
_9MB_SIZE = 9 * 1024 * 1024

# # Resumable Upload
# bucket = client.bucket(BUCKET_NAME)
# blob = bucket.blob(BLOB_NAME)
# blob.chunk_size = _RESUMABLE_UPLOAD_CHUNK_SIZE
# blob.upload_from_filename(LOCAL_FILE_NAME)
# print(f"Succesfully uploaded blob {BLOB_NAME}")

# Resumable Upload
# import os
# bucket = client.create_bucket(BUCKET_NAME)
# blob = bucket.blob(BLOB_NAME)
# blob.chunk_size = _RESUMABLE_UPLOAD_CHUNK_SIZE
# blob.upload_from_string(os.urandom(_9MB_SIZE))
# print(f"Succesfully uploaded blob {BLOB_NAME}")

# Simple Upload
# bucket = client.bucket(BUCKET_NAME)
# blob = bucket.blob(BLOB_NAME_2)
# blob.upload_from_string("hello world")
# print(f"Succesfully uploaded blob {BLOB_NAME}")

# # Download
# blob2 = bucket.blob(BLOB_NAME_2)
# blob2.download_as_bytes()
# print(f"Succesfully downloaded blob")