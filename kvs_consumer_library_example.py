# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

"""
Example to demonstrate usage the AWS Kinesis Video Streams (KVS) Consumer Library for Python.
"""

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import os
import time
from typing import Protocol
import boto3
from sys import argv
from pathlib import Path
import ebmlite

from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import (
    KVSParser,
)
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import (
    KvsFragmentProcessor,
)

from custom_logging import get_full_logger


log = get_full_logger(__name__)
# Ensure directory for audio fragments
root = Path(__file__).parent
audio_path = root / "audio_fragments"
if not audio_path.is_dir():
    if audio_path.exists():
        audio_path.unlink()
    audio_path.mkdir()


# Update the desired region and KVS stream name.
REGION = "eu-west-2"
try:
    KVS_STREAM01_NAME = argv[1]  # Stream must be in specified region
except IndexError:
    log.error("No stream name specified!", exc_info=True)
    raise


class KvsPythonConsumerExample:
    """
    Example class to demonstrate usage the AWS Kinesis Video Streams
    KVS) Consumer Library for Python.
    """

    def __init__(self):

        # Create shared instance of KvsFragmentProcessor
        self.kvs_fragment_processor = KvsFragmentProcessor()

        # Variable to maintain state of last good fragment mostly for
        # error and exception handling.
        self.last_good_fragment_tags = None

        # Init the KVS Service Client and get the accounts KVS service
        # endpoint
        log.info("Initializing Amazon Kinesis Video client....")
        # Attach session specific configuration (such as the
        # authentication pattern)
        self.session = boto3.Session(region_name=REGION)
        self.kvs_client = self.session.client("kinesisvideo")
        self.stream_active = False

    ####################################################
    # Main process loop
    def service_loop(self):
        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a
        # Kinesis Video Stream

        # Initialize an instance of the KvsConsumerLibrary, provide the
        # GetMedia response and the required call-backs
        log.info(f"Starting KvsConsumerLibrary for stream: {KVS_STREAM01_NAME}........")
        my_stream01_consumer = KVSParser(
            kvs_stream_name=KVS_STREAM01_NAME,
            consumer=self
        )

        my_stream01_consumer.start()
        self.stream_active = True

        while self.stream_active:
            # Add Main process / application logic here while
            # KvsConsumerLibrary instance runs as a thread
            log.info("I could be dispatching chunks of data or something")
            time.sleep(5)

            # Call below to exit the streaming get_media() thread
            # gracefully before reaching end of stream.

            # my_stream01_consumer.stop_thread()
        log.info("Probably do some final processing of the last chunk section")

    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(
        self,
        stream_name: str,
        fragment_bytes: bytes,
        fragment_dom: ebmlite.Document,
        time_taken_to_fetch_frag: float
    ):

        try:
            log.info(f"Fragment Received on Stream: {stream_name}")
            log.info(f"time to acquire:{time_taken_to_fetch_frag} ")

            # Get the fragment tags and save in local parameter.
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(
                fragment_dom
            )

            save_dir = str(audio_path)
            wav_file_base_name = self.last_good_fragment_tags[
                "AWS_KINESISVIDEO_FRAGMENT_NUMBER"
            ]
            wav_file_base_path = os.path.join(save_dir, wav_file_base_name)

            log.info(f'Saving audio from client: {wav_file_base_path}')
            self.kvs_fragment_processor.save_connect_fragment_audio_track_from_customer_as_wav(
                fragment_dom, wav_file_base_path
            )
            log.info(f'Saving audio to client: {wav_file_base_path}')
            self.kvs_fragment_processor.save_connect_fragment_audio_track_to_customer_as_wav(
                fragment_dom, wav_file_base_path
            )

        except Exception:
            log.exception("on_fragment_arrived Error")

    def on_stream_read_complete(self, stream_name):

        # Do something here to tell the application that reading from
        # the stream ended gracefully.
        self.stream_active = False
        log.info(f"Read Media on stream: {stream_name} Completed successfully")
        log.info(f"Last Fragment Tags: {self.last_good_fragment_tags}")

    def on_stream_read_exception(self, stream_name, exc):
        self.stream_active = False
        # Here we just log the error
        print(
            f"####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{exc}"
        )


if __name__ == "__main__":
    """
    Main method for example KvsConsumerLibrary
    """

    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()
