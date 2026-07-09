# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

"""
Amazon Kinesis Video Stream (KVS) Consumer Library for Python.

This library parses streaming bytes (chunks) made available by the
StreamingBody returned from calls to the KVS Media Client GetMedia and
KVS Archive Media Client GetMediaForFragmentList API.

The Amazon Kinesis Video Stream (KVS) Consumer Library for Python reads
in streaming bytes as they become available and parses to individual MKV
fragments. The library is threaded and non-blocking, once a stream is
being read it forwards received MKV fragments to named call-backs in
the users application.

Fragments are returned as raw bytes and a searchable DOM like structure
by parsing with EMBLite by MideTechnology.

The consumer library provides the following functions to further process
parsed MKV fragments:
1) get_fragment_tags():
        Extract MKV tags from the fragment.
2) save_fragment_as_local_mkv():
        Saves the fragment as stand-alone MKV file on local disk.
3) get_frames_as_ndarray():
        Returns a ratio of frames in the fragment as a list of NDArray
        objects.
4) save_frames_as_jpeg():
        Returns a ratio of frames in the fragment as a JPEGs to local
        disk.

Workflow:
1) Define a on_fragment_arrived and on_read_stream_complete call-backs
    in user application logic. These to process fragments as they are
    received and to handle the parser reaching the end of the stream.
    (When no more fragments are left),
2) Initialize the KVS Media and / or Archive Media clients,
3) Make a call to KVS Media GetMedia and / or KVS Archive Media
    GetMediaForFragmentList for the given stream,
4) Initialize this KVS Consumer library and call
    get_streaming_fragements providing the response from the GetMedia or
    GetMediaForFragmentList call,
5) Fragments will then be parsed and delivered to the call-backs for
    processing as per the example code provided.
"""

from time import perf_counter
from io import BytesIO
import logging
from threading import Thread
import ebmlite
from boto3 import Session
from botocore.response import StreamingBody
from .kinesis_video_stream_consumer import KVSConsumer

# Init the logger.
log = logging.getLogger(__name__)


class StreamAcquisitionError(Exception):
    """Unable to acquire media stream from Kinesis Video Stream"""


class KVSParser(Thread):
    def __init__(
        self,
        kvs_stream_arn: str,
        start_frag: str,
        consumer: KVSConsumer,
    ):
        # Call the Thread class's init function
        super().__init__()

        # Used to trigger graceful exit of this thread
        self._stop_get_media = False

        # Init the local vars.
        match kvs_stream_arn.split('/'):
            # Attempt at pulling stream name from arn, no biggie though
            # it's only used for telemetry
            case [_, stream_name, _]:
                self.kvs_stream_name = stream_name
            case _:
                self.kvs_stream_name = kvs_stream_arn
        log.info("Initialising KVSParser...")
        self.consumer = consumer
        self.kvs_streaming_buffer = self.__acquire_stream(
            kvs_stream_arn=kvs_stream_arn,
            start_frag=start_frag
        )

        log.info("Loading EBMLlite MKV Schema....")
        self.schema = ebmlite.loadSchema("matroska.xml")
        master = self.schema.elementsByName.get("EBML")
        if not master:
            raise KeyError("Could not find master element in Matroska schema")
        self.matroska_master_element_type = master

    def __acquire_stream(
            self,
            kvs_stream_arn: str,
            start_frag: str
    ) -> StreamingBody:
        """Using the name of the Kinesis Video Stream, this function
        acquires a botocore.response.StreamingBody which can then stream
        the data out of KVS"""
        # First, we need a client for KVS
        session = Session(region_name='eu-west-2')
        kvs_client = session.client("kinesisvideo")

        # Second, we need an endpoint for our specific stream
        get_data_endpoint_response = kvs_client.get_data_endpoint(
            StreamARN=kvs_stream_arn,
            APIName="GET_MEDIA"
        )
        get_media_endpoint = get_data_endpoint_response.get("DataEndpoint")
        if not get_media_endpoint:
            raise StreamAcquisitionError("No data endpoint identified")

        # Third, we can get a client representing our specific stream
        kvs_media_client = session.client(
            "kinesis-video-media", endpoint_url=get_media_endpoint
        )

        # Fourth, we can initiate the streaming of data and pass back
        # the streaming body
        get_media_response_object = kvs_media_client.get_media(
            StreamARN=kvs_stream_arn,
            # TODO: start selector can't be earliest as quick end of one
            # TODO: ...chat and start of another can result in multiple
            # TODO: ...conversations being on one stream (i.e. stream
            # TODO: ...reuse). Must obey start fragment selector.
            # N.B. This could cause problems with over reading and
            # reading the next audio stream.
            StartSelector={
                "StartSelectorType": "FRAGMENT_NUMBER",
                "AfterFragmentNumber": start_frag,
            }
        )
        match get_media_response_object:
            case {"Payload": streamer} if isinstance(streamer, StreamingBody):
                return streamer
            case _:
                raise StreamAcquisitionError("No streaming body presented")

    def _get_ebml_header_elements(
        self, fragement_dom: ebmlite.Document
    ) -> list[ebmlite.Element]:
        """
        Returns the EBML Header elements in the Fragment DOM. EBML
        Header elements indicate the start  of a new fragment and so we
        use them to set the byte boundaries of individual fragments as
        they arrive in the raw data stream (chunks).

        ### Parameters:

            **fragment_dom**: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                The DOM like structure describing the fragment parsed
                by EBMLite.
        """
        ebml_header_elements = []
        # Iterate through the fragment elements and capture any EBML
        # Fragment headers (indicating the start of a new fragment)
        for element in fragement_dom:
            if isinstance(element, self.matroska_master_element_type):
                ebml_header_elements.append(element)

        return ebml_header_elements

    def stop_thread(self):
        self._stop_get_media = True

    ####################################################
    # Read and parse streaming media from a Kinesis Video Stream
    def run(self):
        """
        Reads in chunks (unframed number of raw bytes) from a KVS
        GetMedia or GetMediaForFragmentList Streaming Body response and
        parses into bounded MKV fragments. Raw data is buffered until a
        complete fragment is received which is then forwarded to the
        on_fragmemt_arrived callback. Fragment is delivered as a raw
        byte array and also a parsed EBMLite Document that is a DOM like
        structure of the elements (including Tags) within the given
        Fragment.

        Kinesis Video will continually update the streaming buffer with
        media as soon as its available. For StartSelectorType = NOW,
        bytes from the media stream will be available as fast as they
        arrive into Kinesis Video by the producer. In this case the
        consumer bandwidth and fragment rate will be equal to that of
        the producer. However, if StartSelector is set to sometime in
        the past then all fragments from start to end time will be
        available immediately. The effect is this will read in bytes as
        fast as the system resources (KVS limits, CPU and bandwidth)
        will allow until the stream has caught up with the leading edge
        of media being generated.
        """

        try:

            #########################################
            # Iterate through reading and parsing streaming body
            # response of KVS GET Media API call to MKV fragments.
            #########################################
            chunk_buffer = bytearray()
            fragment_read_start_time = perf_counter()

            chunk_read_count = 0

            # Uses the StreamingBody object iterator to read in (default
            # 1024 byte) chunks from the streaming buffer.
            for chunk in self.kvs_streaming_buffer:
                if self._stop_get_media:
                    break

                # Append chunk bytes to ByteArray buffer while waiting
                # for the entire MKV fragment to arrive.
                chunk_buffer.extend(chunk)

                #############################################
                # Parse current byte buffer to MKV EBML DOM like object
                # using EBMLite
                #############################################
                fragement_intrum_dom = self.schema.load(BytesIO(chunk_buffer), headers=True)

                #############################################
                #  Process a complete fragment if its arrived and send
                # to the on_fragment_arrived callback.
                #############################################
                # EBML header elements indicate the start of a new
                # fragment. Here we check if the start of a second
                # fragment has arrived and use its start to identify the
                # byte boundary of the first complete fragment to
                # process.
                ebml_header_elements = self._get_ebml_header_elements(fragement_intrum_dom)

                # If multiple fragment headers then the first fragment
                # has been received completely and ready to process.
                if len(ebml_header_elements) > 1:
                    # Get the offset for the first and second fragments.
                    # First fragment offset should be zero or fragment
                    # boundary is out of sync!
                    first_ebml_header_offset = ebml_header_elements[0].offset
                    second_ebml_header_offset = ebml_header_elements[1].offset

                    # Isolate the bytes from the first complete MKV
                    # fragments in the received chunk data
                    fragment_bytes = chunk_buffer[
                        first_ebml_header_offset:second_ebml_header_offset
                    ]

                    # Parse the complete fragment as EBML to a DOM like
                    # object
                    fragment_dom = self.schema.load(BytesIO(fragment_bytes), headers=True)

                    # Calculate duration taken receiving this fragment
                    # - just for telemetry of the streaming data.
                    fragment_receive_duration = perf_counter() - fragment_read_start_time

                    # Forward fragment to the on_fragment_arrived
                    # callback.
                    self.consumer.on_fragment_arrived(
                        stream_name=self.kvs_stream_name,
                        fragment_bytes=fragment_bytes,
                        fragment_dom=fragment_dom,
                        time_taken_to_fetch_frag=fragment_receive_duration
                    )

                    # Remove the processed MKV segment from the raw byte
                    # chunk_buffer
                    chunk_buffer = chunk_buffer[second_ebml_header_offset:]

                    # Reset the chunk read count.
                    chunk_read_count = 0

                    # Reset the start time for the next segment
                    # iteration just to time fragment durations
                    fragment_read_start_time = perf_counter()

                #############################################
                # Increment to chunk read count for this fragment
                chunk_read_count += 1

            #############################################
            # Exit the thread if the stream has no more chunks.
            #############################################
            # call the on_stream_read_complete() callback and exit the
            # thread.
            self.consumer.on_stream_read_complete(
                stream_name=self.kvs_stream_name,
            )

        except Exception as err:
            # Pass any exceptions to exception callback.
            self.consumer.on_stream_read_exception(
                stream_name=self.kvs_stream_name,
                exc=err
            )
