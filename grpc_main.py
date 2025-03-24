##################################################################
## Using storage v2 gRPC proto stubs to get bucket, read, write ##
##################################################################
import crc32c
import os
import random

from google.storage.v2 import storage_pb2
from google.storage.v2 import storage_pb2_grpc
import google.auth
import google.auth.transport.grpc
import google.auth.transport.requests
from google.api_core import grpc_helpers
### VARIABLES ###
bucket_id = "bucket2c"
# bucket_id = "bucket4allpublic"
bucket_name = f"projects/_/buckets/{bucket_id}"
target = "storage.googleapis.com:443"
auth_scopes = (
    "https://www.googleapis.com/auth/storage",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/devstorage.full_control",
)
# Get credentials and create channel.
credentials, _ = google.auth.default(scopes=auth_scopes)
channel = grpc_helpers.create_channel(
    target, credentials, default_scopes=auth_scopes
)
# Alternative to create a channel
# Get an HTTP request function to refresh credentials.
# request = google.auth.transport.requests.Request()
# channel = google.auth.transport.grpc.secure_authorized_channel(
#     credentials, request, target,
#     ssl_credentials=grpc.ssl_channel_credentials())
# Use the channel to create a stub.
stub = storage_pb2_grpc.StorageStub(channel)
# Set request headers to include `x-goog-request-params`
metadata = [("x-goog-request-params", f"bucket=projects/_/buckets/{bucket_id}")]
### GET BUCKET ###
request = storage_pb2.GetBucketRequest(
    name=bucket_name
)
response = stub.GetBucket(request, metadata=metadata)
print(f"!! got bucket {bucket_name}")


### WRITE OBJECT ###
obj_name = "324write1"
size = 268435456
QUANTUM = 2 * 1024 * 1000
media = os.urandom(size)
requests = []

offset = 0
end = min(QUANTUM, size)
finish_write = end == size
content = media[0:end]
r1 = storage_pb2.WriteObjectRequest(
    write_object_spec=storage_pb2.WriteObjectSpec(
        resource={
            "name": obj_name,
            "bucket": bucket_name,
        },
    ),
    write_offset=offset,
    checksummed_data=storage_pb2.ChecksummedData(
        content=content, crc32c=crc32c.crc32c(content)
    ),
    finish_write=finish_write,
)
requests.append(r1)

while end < size:
    offset = end
    end = min(end + QUANTUM, size)
    finish_write = end == size
    content = media[offset : end]
    req = storage_pb2.WriteObjectRequest(
        write_offset=offset,
        checksummed_data=storage_pb2.ChecksummedData(
            content=content, crc32c=crc32c.crc32c(content)
        ),
        finish_write=finish_write,
    )
    requests.append(req)

def request_generator():
    for request in requests:
        yield request
metadata = [("x-goog-request-params", f"bucket=projects/_/buckets/{bucket_id}")]
response = stub.WriteObject(request_generator(), metadata=metadata)
print(f"!! successfully uploaded {obj_name} to {bucket_name}")


### READ OBJECT ###
read_obj_name = obj_name
request = storage_pb2.ReadObjectRequest(
    bucket=bucket_name,
    object=read_obj_name,
)
stream = stub.ReadObject(request=request, metadata=metadata)
print("!! Reading object")
for response in stream:
    pass
    # print(response)
