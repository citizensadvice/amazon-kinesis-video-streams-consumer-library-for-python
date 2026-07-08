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
    raw_bytes: bytes
    frame_rate: int
    sample_width: int
    num_channels: int
    rms: float
    frag_num: Any

    def __str__(self):
        details = f"{self.frame_rate=} {self.sample_width=} {self.num_channels=} {self.rms=}"
        return f"{hash(details)} - {details}"


class SliceConsumer:
    dispatched_audio_dir: Path = _ROOT / 'dispatches'

    def __init__(
            self,
            min_chunk_length: int = 90,
            max_chunk_length: int = 120,
    ):
        """max length is maximum number of audio fragments to constitute
        a audio chunk, min length is the minimum. It is important to
        have a generous difference to allow as wide a window as possible
        for detection of a quiet period."""
        if min_chunk_length > max_chunk_length:
            raise ValueError("min chunk length must be less than max")
        self.to_client_fragments: list[AudioFragment] = []
        self.from_client_fragments: list[AudioFragment] = []
        self.max_chunk_size = max_chunk_length
        self.min_chunk_size = min_chunk_length
        self.processor = KvsFragmentProcessor()

        # TODO: get shot of this and do timestamp relative to
        # TODO: ...conversation start
        self.to_client_chunk_number = 0
        self.from_client_chunk_number = 0

    @staticmethod
    def audio_loudness(audio: BytesIO) -> float:
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
            self.to_client_chunk_number += 1
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
            self.from_client_chunk_number += 1
            self.dispatch_chunk(from_client_chunk, "from-client")

    def on_stream_read_complete(
            self,
            stream_name: str
    ):
        self._dispatch_remaining()

    def on_stream_read_exception(
            self,
            stream_name: str,
            exc: Exception
    ):
        print(repr(exc))
        self._dispatch_remaining()

    def _dispatch_remaining(self):
        # Need to simply dispatch the last of the audio fragments that
        # weren't chunked up previously
        self.to_client_chunk_number += 1
        self.dispatch_chunk(
            fragments=self.to_client_fragments,
            label="to-client")
        self.from_client_chunk_number += 1
        self.dispatch_chunk(self.from_client_fragments, "from-client")

    def split_chunk(
            self,
            fragments: list[AudioFragment]
    ) -> tuple[list[AudioFragment] | None, list[AudioFragment]]:
        chunk = None
        buffer = fragments.copy()
        if len(buffer) >= self.max_chunk_size:
            # Assume last fragment is quietest
            min_rms = buffer[-1].rms
            min_rms_pos = len(buffer) - 1
            # open up window in which we may make slice
            slice_window = buffer[
                self.min_chunk_size:    # start at min chunk size
                self.max_chunk_size     # end at max chunk size
            ]
            # loop fragments in window
            for frag_pos_in_window, frag in enumerate(slice_window):
                if frag.rms < min_rms:
                    min_rms = frag.rms
                    min_rms_pos = self.min_chunk_size + frag_pos_in_window

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
        self._write_chunk_to_disk(
            chunk=fragments,
            label=label,
        )

    def _extract_fragment_by_track_name(
            self,
            fragment_dom: Document,
            track_name: str
    ) -> AudioFragment:
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
        if not self.dispatched_audio_dir.is_dir():
            self.dispatched_audio_dir.unlink(missing_ok=True)
            self.dispatched_audio_dir.mkdir()

    def _write_chunk_to_disk(
            self,
            chunk: list[AudioFragment],
            label: str,
    ):
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
        with wave.open(audio_file_path, mode="w") as f:
            f.setnchannels(num_channels)
            f.setsampwidth(sample_width)
            f.setframerate(frame_rate)
            f.writeframes(audio_bytes)
