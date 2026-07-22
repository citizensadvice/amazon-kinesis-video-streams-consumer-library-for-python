# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

"""
Example to demonstrate usage the AWS Kinesis Video Streams (KVS) Consumer Library for Python.
"""

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import time
from sys import argv
from logging import getLogger

from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import (
    KVSParser,
)
from audio_slice_consumer import SliceConsumer
from custom_logging import set_up_root_logger_output


log = getLogger(__name__)

try:
    KVS_STREAM_ARN = argv[1]  # Stream must be in specified region
except IndexError:
    log.error("No stream name specified!")
    raise

try:
    START_FRAG = argv[2]
except IndexError:
    log.error("Must specify a start fragment!")
    raise


def main():
    log.info(f"Starting KvsConsumerLibrary for stream: {KVS_STREAM_ARN}........")

    # This object defines how audio fragments are handled
    consumer = SliceConsumer(
        # These have defaults but can be manually edited here
        min_chunk_size_in_kb=500,
        max_chunk_size_in_kb=800,
    )

    # This object handles the streaming and yielding of audio fragments
    # from the kinesis stream
    my_stream01_consumer = KVSParser(
        kvs_stream_arn=KVS_STREAM_ARN,
        start_frag=START_FRAG,
        consumer=consumer
    )

    my_stream01_consumer.start()
    my_stream01_consumer.join()
    log.info("Probably do some final processing of the last chunk section")


if __name__ == "__main__":
    set_up_root_logger_output()
    main()
