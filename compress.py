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


class StreamProcessor:
    audio_output_dir: Path = _root / 'compressions'
    audio_input_dir: Path = _root / 'audio_fragments'
    rms_threshold: float = 0.01

    def __init__(self):
        self.to_client_fragments: list[AudioFragment] = []
        self.from_client_fragments: list[AudioFragment] = []

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

    def _write_audio_out(self, fragments: list[AudioFragment], label: str):
        chunks: list[bytes] = []
        ready_to_chunk = False
        chunk = bytes()
        num_channels = fragments[0].num_channels
        sample_width = fragments[0].sample_width
        frame_rate = fragments[0].frame_rate
        for frag in fragments:
            chunk += frag.raw_bytes
            if frag.rms > self.rms_threshold:
                ready_to_chunk = True
            else:
                if ready_to_chunk:
                    chunks.append(chunk)
                    ready_to_chunk = False
                    chunk = bytes()
        for idx, chunk in enumerate(chunks):
            with wave.open(str(self.audio_output_dir / f"{label}-{idx}.wav"), mode="w") as f:
                f.setnchannels(num_channels)
                f.setsampwidth(sample_width)
                f.setframerate(frame_rate)
                f.writeframes(chunk)

    def write_audio(self):
        self._write_audio_out(self.from_client_fragments, 'from-client')
        self._write_audio_out(self.to_client_fragments, 'to-client')


sp = StreamProcessor()

sp.process_input_fragments()
for frag in sp.from_client_fragments:
    print(frag)
sp.write_audio()
