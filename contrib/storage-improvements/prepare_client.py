"""Extract a checksum-pinned native client wheel for the Linux proof."""
import hashlib
import json
import os
from pathlib import Path
import platform
import urllib.request
import zipfile

if platform.system() != 'Linux' or platform.machine() not in ('aarch64', 'arm64'):
    raise SystemExit('Proof client currently targets Linux ARM64')
root = Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'zenoh-fork/client-1.10.0'
root.mkdir(parents=True, exist_ok=True)
lock = json.loads(Path(__file__).with_name('client.json').read_text())['python']
wheel = root / lock['filename']
if not wheel.exists() or hashlib.sha256(wheel.read_bytes()).hexdigest() != lock['sha256']:
    with urllib.request.urlopen(lock['url'], timeout=60) as response:
        payload = response.read()
    if hashlib.sha256(payload).hexdigest() != lock['sha256']:
        raise RuntimeError('Client wheel checksum mismatch')
    wheel.write_bytes(payload)
with zipfile.ZipFile(wheel) as archive:
    for entry in archive.infolist():
        if entry.is_dir(): continue
        relative = Path(entry.filename)
        if relative.is_absolute() or '..' in relative.parts:
            raise RuntimeError('Unsafe wheel entry')
        target = root / 'python' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(archive.read(entry))
print(root / 'python')
