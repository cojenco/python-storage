# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Workload W1R3 profiling script. This is not an officially supported Google product."""

import logging
import os
import random
import time
import uuid

from functools import partial, update_wrapper

from google.cloud import storage

import _perf_utils as _pu

### TEMP ADDED FOR GRPC ###
import crc32c
from google.storage.v2 import storage_pb2
from google.storage.v2 import storage_pb2_grpc
import google.auth
import google.auth.transport.grpc
import google.auth.transport.requests
from google.api_core import grpc_helpers
### TEMP ADDED FOR GRPC ###


def WRITE(bucket, blob_name, checksum, size, args, **kwargs):
    """Perform an upload and return latency."""
    # blob = bucket.blob(blob_name)
    # file_path = f"{os.getcwd()}/{uuid.uuid4().hex}"
    # # Create random file locally on disk
    # with open(file_path, "wb") as file_obj:
    #     file_obj.write(os.urandom(size))

    ### TEMP CREATE GRPC STUB ###
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
    stub = storage_pb2_grpc.StorageStub(channel)
    ### TEMP CREATE GRPC STUB ###


    ### WRITE OBJECT ###
    bucket_id = bucket.name
    bucket_name = f"projects/_/buckets/{bucket_id}"
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
                "name": blob_name,
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

    start_time = time.monotonic_ns()
    metadata = [("x-goog-request-params", f"bucket=projects/_/buckets/{bucket_id}")]
    _ = stub.WriteObject(request_generator(), metadata=metadata)
    end_time = time.monotonic_ns()


    elapsed_time = round(
        (end_time - start_time) / 1000
    )  # convert nanoseconds to microseconds

    return elapsed_time


def READ(bucket, blob_name, checksum, args, **kwargs):
    """Perform a download and return latency."""

    range_read_size = args.range_read_size
    range_read_offset = kwargs.get("range_read_offset")
    # Perfor range read if range_read_size is specified, else get full object.
    if range_read_size != 0:
        start = range_read_offset
        end = start + range_read_size - 1
    else:
        start = 0
        end = -1


    ### TEMP CREATE GRPC STUB ###
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
    stub = storage_pb2_grpc.StorageStub(channel)
    ### TEMP CREATE GRPC STUB ###

    ### READ OBJECT ###
    bucket_id = bucket.name
    bucket_name = f"projects/_/buckets/{bucket_id}"
    metadata = [("x-goog-request-params", f"bucket=projects/_/buckets/{bucket_id}")]
    read_obj_name = blob_name

    start_time = time.monotonic_ns()
    request = storage_pb2.ReadObjectRequest(
        bucket=bucket_name,
        object=read_obj_name,
    )
    stream = stub.ReadObject(request=request, metadata=metadata)
    list(stream)
    end_time = time.monotonic_ns()

    elapsed_time = round(
        (end_time - start_time) / 1000
    )  # convert nanoseconds to microseconds

    return elapsed_time


def _wrapped_partial(func, *args, **kwargs):
    """Helper method to create partial and propagate function name and doc from original function."""
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


def _generate_func_list(args):
    """Generate Write-1-Read-3 workload."""
    bucket_name = args.bucket
    blob_name = f"{_pu.TIMESTAMP}-{uuid.uuid4().hex}"

    # parse min_size and max_size from object_size
    min_size, max_size = _pu.get_min_max_size(args.object_size)
    # generate randmon size in bytes using a uniform distribution
    size = random.randint(min_size, max_size)

    # generate random checksumming type: md5, crc32c or None
    idx_checksum = random.choice([0, 1, 2])
    checksum = _pu.CHECKSUM[idx_checksum]

    # generated random read_offset
    range_read_offset = random.randint(
        args.minimum_read_offset, args.maximum_read_offset
    )

    func_list = [
        _wrapped_partial(
            WRITE,
            storage.Client().bucket(bucket_name),
            blob_name,
            size=size,
            checksum=checksum,
            args=args,
        ),
        *[
            _wrapped_partial(
                READ,
                storage.Client().bucket(bucket_name),
                blob_name,
                size=size,
                checksum=checksum,
                args=args,
                num=i,
                range_read_offset=range_read_offset,
            )
            for i in range(3)
        ],
    ]
    return func_list


def log_performance(func, args, elapsed_time, status, failure_msg):
    """Hold benchmarking results per operation call."""
    size = func.keywords.get("size")
    checksum = func.keywords.get("checksum", None)
    num = func.keywords.get("num", None)
    range_read_size = args.range_read_size

    res = {
        "Op": func.__name__,
        "ElapsedTimeUs": elapsed_time,
        "ApiName": args.api,
        "RunID": _pu.TIMESTAMP,
        "CpuTimeUs": _pu.NOT_SUPPORTED,
        "AppBufferSize": _pu.NOT_SUPPORTED,
        "LibBufferSize": _pu.DEFAULT_LIB_BUFFER_SIZE,
        "ChunkSize": 0,
        "ObjectSize": size,
        "TransferSize": size,
        "TransferOffset": 0,
        "RangeReadSize": range_read_size,
        "BucketName": args.bucket,
        "Library": "python-storage",
        "Crc32cEnabled": checksum == "crc32c",
        "MD5Enabled": checksum == "md5",
        "FailureMsg": failure_msg,
        "Status": status,
    }

    if res["Op"] == "READ":
        res["Op"] += f"[{num}]"

        # For range reads (workload 2), record additional outputs
        if range_read_size > 0:
            res["TransferSize"] = range_read_size
            res["TransferOffset"] = func.keywords.get("range_read_offset", 0)

    return res


def run_profile_w1r3(args):
    """Run w1r3 benchmarking. This is a wrapper used with the main benchmarking framework."""
    results = []

    for func in _generate_func_list(args):
        failure_msg = ""
        try:
            elapsed_time = func()
        except Exception as e:
            failure_msg = (
                f"Caught an exception while running operation {func.__name__}\n {e}"
            )
            logging.exception(failure_msg)
            status = ["FAIL"]
            elapsed_time = _pu.NOT_SUPPORTED
        else:
            status = ["OK"]

        res = log_performance(func, args, elapsed_time, status, failure_msg)
        results.append(res)

    return results


def run_profile_range_read(args):
    """Run range read W2 benchmarking. This is a wrapper used with the main benchmarking framework."""
    results = []

    for func in _generate_func_list(args):
        failure_msg = ""
        try:
            elapsed_time = func()
        except Exception as e:
            failure_msg = (
                f"Caught an exception while running operation {func.__name__}\n {e}"
            )
            logging.exception(failure_msg)
            status = ["FAIL"]
            elapsed_time = _pu.NOT_SUPPORTED
        else:
            status = ["OK"]

    # Only measure the last read
    res = log_performance(func, args, elapsed_time, status, failure_msg)
    results.append(res)

    return results
