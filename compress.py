import wave
from pathlib import Path

root = Path(__file__).parent
audio_dir = root / "audio_fragments"
compression_dir = root / "compressions"
if not compression_dir.is_dir():
    compression_dir.unlink(missing_ok=True)
    compression_dir.mkdir()

# Defaults (overwritten later)
frame_rate = 16_000
sample_width = 2
num_channels = 1

from_client = bytes()
to_client = bytes()
for working_dir, nest_dirs, files in audio_dir.walk():
    for file in sorted(files):
        with wave.open(str(working_dir / file), mode="r") as f:
            audio_bytes = f.readframes(f.getnframes())
            frame_rate = f.getframerate()
            sample_width = f.getsampwidth()
            num_channels = f.getnchannels()
        if "FROM_CUSTOMER" in file:
            from_client += audio_bytes
        else:
            to_client += audio_bytes


with wave.open(str(compression_dir / "client.wav"), mode="w") as f:
    f.setnchannels(num_channels)
    f.setsampwidth(sample_width)
    f.setframerate(frame_rate)
    f.writeframes(from_client)

with wave.open(str(compression_dir / "non-client.wav"), mode="w") as f:
    f.setnchannels(num_channels)
    f.setsampwidth(sample_width)
    f.setframerate(frame_rate)
    f.writeframes(to_client)
