"""This module only defines the Protocol for being a consumer of Kinesis
Video Streams that can be plugged into the KVSParser"""

from typing import Protocol
from ebmlite import Document


class KVSConsumer(Protocol):
    def on_fragment_arrived(
        self,
        stream_name: str,
        fragment_bytes: bytes,
        fragment_dom: Document,
        time_taken_to_fetch_frag: float
    ):
        """
        This is the callback for the KvsConsumerLibrary to send MKV
        fragments as they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw
        bytes and a DOM like structure containing the fragments meta
        data.

        With these parameters you can do a variety of post-processing
        including saving the fragment as a standalone MKV file to local
        disk, request individual frames as a numpy.ndarray for data
        science applications or as JPEG/PNG files to save to disk or
        pass to computer vision solutions. Finally, you can also use the
        Fragment DOM to access Meta-Data such as the MKV tags as well as
        track ID and codec information.

        In the below example we provide a demonstration of all of these
        described functions.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary
                thread triggering this callback was initiated. Use this
                to identify a fragment when multiple streams are read
                from different instances of KvsConsumerLibrary to this
                callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment.
                Can be save or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document
                        <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing
                searchable list of EBML elements and MetaData in the
                Fragment

            **time_taken_to_fetch_frag**: float
                The time in seconds that the fragment took for the
                streaming data to be received and processed.
        """

    def on_stream_read_complete(
            self,
            stream_name: str
    ):
        """
        This callback is triggered by the KvsConsumerLibrary when a
        stream has no more fragments available. This represents a
        graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the
        StreamSelector applied some time or fragment bounding on the
        media request or if requesting a live steam and the producer
        stopped sending more fragments.

        Here you can choose to either restart reading the stream at a
        new time or just clean up any resources that were expecting to
        process any further fragments.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary
                thread triggering this callback was initiated. Use this
                to identify a fragment when multiple streams are read
                from different instances of KvsConsumerLibrary to this
                callback.
        """

    def on_stream_read_exception(
            self,
            stream_name: str,
            exc: Exception
    ):
        """
        This callback is triggered by an exception in the
        KvsConsumerLibrary reading a stream.

        For example, to process use the last good fragment number from
        self.last_good_fragment_tags to restart the stream from that
        point in time with the example stream selector provided below.

        Alternatively, just handle the failed stream as per your
        application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary
                thread triggering this callback was initiated. Use this
                to identify a fragment when multiple streams are read
                from different instances of KvsConsumerLibrary to this
                callback.

            **exc**: Exception
                The Exception object that was thrown to trigger this
                callback.
        """
