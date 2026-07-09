"""This module defines a consumer of the KVSConsumer protocol"""

from dataclasses import dataclass
from math import sqrt
from io import BytesIO
from typing import Any
import wave
from pathlib import Path
from logging import getLogger

from ebmlite import Document
import librosa
import numpy as np

from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragmentProcessor


log = getLogger(__name__)

_ROOT = Path(__file__).parent


@dataclass
class AudioFragment:
    """Represents a fragment of audio retrieved from a KVS Stream"""
    raw_bytes: bytes
    frame_rate: int
    sample_width: int
    num_channels: int
    rms: float
    frag_num: Any

    def __str__(self):
        details = f"{self.frame_rate=} {self.sample_width=} {self.num_channels=} {self.rms=}"
        return f"{hash(details)} - {details}"

    def size_in_mb(self) -> float:
        # Check if attr set, if not set it
        if not hasattr(self, 'bytes_in_mb'):
            self.bytes_in_mb = 1 << 20
        return len(self.raw_bytes) / self.bytes_in_mb


class SliceConsumer:
    """Based on the KVSConsumer protocol, this class can receive and
    dispatch chunks of audio in real time"""
    dispatched_audio_dir: Path = _ROOT / 'dispatches'

    def __init__(
            self,
            min_chunk_size_in_mb: float = 90.0,
            max_chunk_size_in_mb: float = 180.0,
    ):
        """min/max chunk size refers to dispatched audio chunks. It is
        important to have a generous difference to allow as wide a
        window as possible for detection of a quiet period."""
        if min_chunk_size_in_mb > max_chunk_size_in_mb:
            raise ValueError("min chunk length must be less than max")
        self.to_client_fragments: list[AudioFragment] = []
        self.from_client_fragments: list[AudioFragment] = []
        self.min_chunk_size_in_mb = min_chunk_size_in_mb
        self.max_chunk_size_in_mb = max_chunk_size_in_mb
        self.processor = KvsFragmentProcessor()

    @staticmethod
    def audio_loudness(audio: BytesIO) -> float:
        """calculates the root mean square of a audio fragment i.e.
        absolute loudness"""
        audio.seek(0)
        y, _ = librosa.load(audio, sr=None)
        # Calculate RMS energy for the entire signal
        #   - indicator for loudness
        rms = sqrt(np.mean(y**2))
        return rms

    def on_fragment_arrived(
        self,
        stream_name: str,
        fragment_bytes: bytes,
        fragment_dom: Document,
        time_taken_to_fetch_frag: float
    ):
        """part of KVSConsumer protocol, handles incoming audio
        fragments and dispatching audio chunks when enough fragments
        arrive"""
        # TODO: get shot of this and do timestamp relative to
        # TODO: ...conversation start instead of frag num
        frag_num = self.processor.get_fragment_tags(fragment_dom)[
                "AWS_KINESISVIDEO_FRAGMENT_NUMBER"
            ]
        log.info(f"Received fragment {frag_num!r}")
        # ====================
        # TO CLIENT HANDLING
        to_client_frag = self._extract_fragment_by_track_name(
            fragment_dom, "AUDIO_TO_CUSTOMER"
        )
        log.info(f"{str(to_client_frag)=}")
        self.to_client_fragments.append(to_client_frag)
        to_client_chunk, self.to_client_fragments = self.split_chunk(
            self.to_client_fragments
        )
        if to_client_chunk:
            self.dispatch_chunk(to_client_chunk, "to-client")

        # ====================
        # FROM CLIENT HANDLING
        from_client_frag = self._extract_fragment_by_track_name(
            fragment_dom, "AUDIO_FROM_CUSTOMER"
        )
        log.info(f"{str(from_client_frag)=}")
        self.from_client_fragments.append(from_client_frag)
        from_client_chunk, self.from_client_fragments = self.split_chunk(
            self.from_client_fragments
        )
        if from_client_chunk:
            self.dispatch_chunk(from_client_chunk, "from-client")

    def on_stream_read_complete(
            self,
            stream_name: str
    ):
        """part of KVSConsumer protocol, dispatches remaining chunks
        when stream read completes"""
        self._dispatch_remaining()

    def on_stream_read_exception(
            self,
            stream_name: str,
            exc: Exception
    ):
        """part of KVSConsumer protocol, dispatches remaining chunks
        when stream read fails"""
        print(repr(exc))
        self._dispatch_remaining()

    def _dispatch_remaining(self):
        """private method to flush remaining fragments into dispatch"""
        # Need to simply dispatch the last of the audio fragments that
        # weren't chunked up previously
        self.dispatch_chunk(
            fragments=self.to_client_fragments,
            label="to-client")
        self.dispatch_chunk(self.from_client_fragments, "from-client")

    def split_chunk(
            self,
            fragments: list[AudioFragment]
    ) -> tuple[list[AudioFragment] | None, list[AudioFragment]]:
        """splits large list of fragments into a chunk and the remaining
        fragments, the slice is based on the quietest period in the
        min/max chunk size window"""
        # First double check if fragments are large enough to chunk
        # if not, return no chunk and original fragments
        if sum([frag.size_in_mb() for frag in fragments]) < self.max_chunk_size_in_mb:
            return None, fragments

        # Here we chunk off some fragments, starting by making a copy
        # for reasons of immutability of inputs
        buffer = fragments.copy()
        # Assume last fragment is quietest
        min_rms = buffer[-1].rms
        min_rms_pos = len(buffer) - 1

        # open up window in which we may make slice

        # calculate window positions
        min_window_pos = None
        max_window_pos = None
        total_buffer_size_so_far = 0
        for idx_of_frag, fragment in enumerate(buffer):
            total_buffer_size_so_far += fragment.size_in_mb()
            if (
                total_buffer_size_so_far > self.min_chunk_size_in_mb
                and min_window_pos is None
            ):
                min_window_pos = idx_of_frag
            if (
                total_buffer_size_so_far >= self.max_chunk_size_in_mb
                and max_window_pos is None
            ):
                max_window_pos = idx_of_frag

        # check window positions generated successfully
        if (
            min_window_pos is None
            or max_window_pos is None
        ):
            # TODO: Obviously handle that cleaner, ideally no exception
            raise Exception("Can't see out that window mate!!")

        slice_window = buffer[
            min_window_pos:    # start at min chunk size (inclusive)
            max_window_pos     # end at max chunk size (exclusive)
        ]
        # loop fragments in window
        for frag_pos_in_window, frag in enumerate(slice_window):
            if frag.rms < min_rms:
                min_rms = frag.rms
                min_rms_pos = min_window_pos + frag_pos_in_window

        # Acquire what we want to write
        chunk = buffer[:min_rms_pos]
        # Remove what we want to write from buffer
        buffer = buffer[min_rms_pos:]
        return chunk, buffer

    def dispatch_chunk(
            self,
            fragments: list[AudioFragment],
            label: str
    ):
        """sends a chunk to whatever destination"""
        self._write_chunk_to_disk(
            chunk=fragments,
            label=label,
        )

    def _extract_fragment_by_track_name(
            self,
            fragment_dom: Document,
            track_name: str
    ) -> AudioFragment:
        """extracts a single channel of audio from the EBML DOM"""
        frag_num = self.processor.get_fragment_tags(fragment_dom)[
                "AWS_KINESISVIDEO_FRAGMENT_NUMBER"
            ]
        log.info(f"Extracting track {track_name!r} from {frag_num=}")
        track_number = self.processor.get_track_number_by_name(
            fragment_dom, track_name
        )
        bytes = self.processor.get_track_bytearray(
            fragment_dom, track_number
        )
        log.info(f"received {len(bytes)} bytes of audio from track {track_name!r}")
        wav = self.processor.convert_track_to_wav(bytes)
        wav.seek(0)
        with wave.open(wav, mode='rb') as w:
            frame_rate = w.getframerate()
            sample_width = w.getsampwidth()
            num_channels = w.getnchannels()
        rms = self.audio_loudness(wav)
        return AudioFragment(
            raw_bytes=bytes,
            frame_rate=frame_rate,
            sample_width=sample_width,
            num_channels=num_channels,
            rms=rms,
            frag_num=frag_num
        )

    def _ready_output_dir(self):
        """private method to ensure that the directory we write to disk
        in exists before we try"""
        if not self.dispatched_audio_dir.is_dir():
            self.dispatched_audio_dir.unlink(missing_ok=True)
            self.dispatched_audio_dir.mkdir()

    def _write_chunk_to_disk(
            self,
            chunk: list[AudioFragment],
            label: str,
    ):
        """one dispatch method which dispatches chunks of audio to
        disk"""
        self._ready_output_dir()
        audio_bytes = bytes()
        if not chunk:
            return
        frag_num = chunk[0].frag_num
        for frag in chunk:
            audio_bytes += frag.raw_bytes
        num_channels = chunk[0].num_channels
        sample_width = chunk[0].sample_width
        frame_rate = chunk[0].frame_rate
        audio_file_path = str(self.dispatched_audio_dir / f"{label}-{frag_num}.wav")
        log.info(f"writing: {audio_file_path!r}")
        bytes_in_mb = 2 ** 20
        log.info(f"Estimate {len(audio_bytes) / bytes_in_mb}Mb for {audio_file_path}")
        with wave.open(audio_file_path, mode="w") as f:
            f.setnchannels(num_channels)
            f.setsampwidth(sample_width)
            f.setframerate(frame_rate)
            f.writeframes(audio_bytes)
