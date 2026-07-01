from pathlib import Path
import librosa
from math import sqrt
import numpy as np
import wave


def audio_loudness(audio_path: Path) -> float:
    y, sr = librosa.load(audio_path, sr=None)
    # Calculate RMS energy for the entire signal
    #   - indicator for loudness
    rms = sqrt(np.mean(y**2))
    return rms


root = Path(__file__).parent
audio_dir = root / "audio_fragments"
compression_dir = root / "compressions"
if not compression_dir.is_dir():
    compression_dir.unlink(missing_ok=True)
    compression_dir.mkdir()


frame_rate = None
sample_width = None
num_channels = None

from_client_chunks: list[bytes] = []
to_client_chunks: list[bytes] = []
from_client = bytes()
to_client = bytes()
to_client_ready_to_chunk = False
from_client_ready_to_chunk = False
rms_threshold = 0.01
for working_dir, nest_dirs, files in audio_dir.walk():
    for file in sorted(files):
        with wave.open(str(working_dir / file), mode="r") as f:
            audio_bytes = f.readframes(f.getnframes())
            frame_rate = frame_rate or f.getframerate()
            sample_width = sample_width or f.getsampwidth()
            num_channels = num_channels or f.getnchannels()
        if "FROM_CUSTOMER" in file:
            from_client += audio_bytes
            # check chunkiness
            rms = audio_loudness(working_dir / file)
            if rms > rms_threshold:
                from_client_ready_to_chunk = True
            else:
                if from_client_ready_to_chunk:
                    from_client_chunks.append(from_client)
                    from_client_ready_to_chunk = False
                    from_client = bytes()
        else:
            to_client += audio_bytes
            # check chunkiness
            rms = audio_loudness(working_dir / file)
            if rms > rms_threshold:
                to_client_ready_to_chunk = True
            else:
                if to_client_ready_to_chunk:
                    to_client_chunks.append(to_client)
                    to_client_ready_to_chunk = False
                    to_client = bytes()
to_client_chunks.append(to_client)
from_client_chunks.append(from_client)

for idx, chunk in enumerate(from_client_chunks):
    with wave.open(str(compression_dir / f"client-{idx}.wav"), mode="w") as f:
        f.setnchannels(num_channels)
        f.setsampwidth(sample_width)
        f.setframerate(frame_rate)
        f.writeframes(chunk)

for idx, chunk in enumerate(to_client_chunks):
    with wave.open(str(compression_dir / f"non-client-{idx}.wav"), mode="w") as f:
        f.setnchannels(num_channels)
        f.setsampwidth(sample_width)
        f.setframerate(frame_rate)
        f.writeframes(chunk)
