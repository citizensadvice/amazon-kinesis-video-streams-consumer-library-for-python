from dataclasses import dataclass
from pathlib import Path
import librosa
from math import sqrt
import numpy as np
import wave

_root = Path(__file__).parent


@dataclass
class AudioFragment:
    raw_bytes: bytes
    frame_rate: int
    sample_width: int
    num_channels: int
    rms: float

    def __str__(self):
        return f"{self.frame_rate=} {self.sample_width=} {self.num_channels=} {self.rms=}"


class AudioFragmentProcessor:
    audio_output_dir: Path = _root / 'compressions'
    audio_input_dir: Path = _root / 'audio_fragments'

    def __init__(
            self,
            min_chunk_length: int = 90,
            max_chunk_length: int = 120,
    ):
        """max length is maximum number of audio fragments to constitute
        a audio chunk, min length is the minimum. It is important to
        have a generous difference to allow as wide a window as possible
        for detection of a quiet period."""
        self.to_client_fragments: list[AudioFragment] = []
        self.from_client_fragments: list[AudioFragment] = []
        self.max_chunk_size = max_chunk_length
        self.min_chunk_size = min_chunk_length

    def ready_output_dir(self):
        if not self.audio_output_dir.is_dir():
            self.audio_output_dir.unlink(missing_ok=True)
            self.audio_output_dir.mkdir()

    @staticmethod
    def audio_loudness(audio_path: Path) -> float:
        y, _ = librosa.load(audio_path, sr=None)
        # Calculate RMS energy for the entire signal
        #   - indicator for loudness
        rms = sqrt(np.mean(y**2))
        return rms

    def process_input_fragments(self):
        for working_dir, nest_dirs, files in self.audio_input_dir.walk():
            for file in sorted(files):
                audio_path = working_dir / file
                with wave.open(str(audio_path), mode="r") as f:
                    audio_bytes = f.readframes(f.getnframes())
                    frame_rate = f.getframerate()
                    sample_width = f.getsampwidth()
                    num_channels = f.getnchannels()
                    rms = self.audio_loudness(audio_path)
                    frag = AudioFragment(
                        raw_bytes=audio_bytes,
                        frame_rate=frame_rate,
                        sample_width=sample_width,
                        num_channels=num_channels,
                        rms=rms,
                    )
                    if "FROM_CUSTOMER" in file:
                        self.from_client_fragments.append(frag)
                    else:
                        self.to_client_fragments.append(frag)

    def _write_audio_out(
            self,
            fragments: list[AudioFragment],
            label: str
    ):
        """Loops through provided audio fragments, filling up a buffer.
        When buffer is full, a window is opened on the last handful of
        fragments. This window is checked for the quietest fragment and
        the chunk buffer is sliced there. Anything before that slice is
        written as a wav file. This looping is continued until all
        fragments are written."""
        # Assign buffer
        chunk_buffer: list[AudioFragment] = []
        chunk_num = 0  # number the chunks
        for frag in fragments:
            # Each frag is added the the buffer
            chunk_buffer.append(frag)
            # When buffer is plump
            if len(chunk_buffer) == self.max_chunk_size:
                # Number chunk
                chunk_num += 1

                # Assume last fragment is quietest
                min_rms = chunk_buffer[-1].rms
                min_rms_pos = len(chunk_buffer) - 1
                # open up window in which we may make slice
                slice_window = chunk_buffer[
                    self.min_chunk_size:    # start at min chunk size
                    self.max_chunk_size     # end at max chunk size
                ]
                # loop fragments in window
                for frag_pos_in_window, frag in enumerate(slice_window):
                    if frag.rms < min_rms:
                        min_rms = frag.rms
                        min_rms_pos = self.min_chunk_size + frag_pos_in_window

                # Acquire what we want to write
                to_write = chunk_buffer[:min_rms_pos]
                # Remove what we want to write from buffer
                chunk_buffer = chunk_buffer[min_rms_pos:]
                self._write_chunk(to_write, label, chunk_num)
        # Write whatever remains in buffer
        self._write_chunk(chunk_buffer, label, chunk_num + 1)

    def _write_chunk(self, chunk: list[AudioFragment], label: str, chunk_num: int):
        audio_bytes = bytes()
        for frag in chunk:
            audio_bytes += frag.raw_bytes
        num_channels = chunk[0].num_channels
        sample_width = chunk[0].sample_width
        frame_rate = chunk[0].frame_rate
        with wave.open(str(self.audio_output_dir / f"{label}-{chunk_num}.wav"), mode="w") as f:
            f.setnchannels(num_channels)
            f.setsampwidth(sample_width)
            f.setframerate(frame_rate)
            f.writeframes(audio_bytes)

    def write_audio(self):
        self.ready_output_dir()
        self._write_audio_out(self.from_client_fragments, 'from-client')
        self._write_audio_out(self.to_client_fragments, 'to-client')


sp = AudioFragmentProcessor()

sp.process_input_fragments()
for frag in sp.from_client_fragments:
    print(frag)
sp.write_audio()
