"""Build images first, then run: python3 examples/file-nodes/test.py."""
import os
from pathlib import Path
import subprocess
import tempfile


def run(image, inputs, outputs, *args, expected=0, env=()):
    command = ["docker", "run", "--rm", "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
               "-v", f"{inputs}:/data/inputs:ro", "-v", f"{outputs}:/data/outputs"]
    for value in env:
        command += ["-e", value]
    result = subprocess.run(command + [f"rayflow-file-{image}:local", *args], capture_output=True, text=True)
    assert result.returncode == expected, (command, result.returncode, result.stdout, result.stderr)
    print(result.stdout.strip() or result.stderr.strip())


with tempfile.TemporaryDirectory(prefix="rayflow-file-nodes-") as temporary:
    root = Path(temporary)
    empty, generated, renamed = [root / name for name in ("empty", "generated", "renamed")]
    for directory in (empty, generated, renamed):
        directory.mkdir()
    run("generate", empty, generated, "first.bin", "nested/second.bin",
        env=("RAYFLOW_FILE_SIZE_BYTES=12582912",))
    assert (generated / "first.bin").stat().st_size == 12582912
    run("rename", generated, renamed, "first.bin", "renamed.bin", "nested/second.bin", "nested/copy.bin")
    assert (generated / "first.bin").exists(), "Read-only input must survive renaming"
    assert (renamed / "renamed.bin").read_bytes() == (generated / "first.bin").read_bytes()
    run("verify", renamed, empty, "renamed.bin", "nested/copy.bin")
    with (renamed / "nested/copy.bin").open("r+b") as output:
        output.write(b"different")
    run("verify", renamed, empty, "renamed.bin", "nested/copy.bin", expected=1)
    run("verify", renamed, empty, "renamed.bin", "missing.bin", expected=2)
    run("generate", empty, generated, "../escape.bin", expected=2)
    run("rename", generated, renamed, "first.bin", expected=2)
print("All three Docker images passed generation, renaming, comparison, and error checks.")
