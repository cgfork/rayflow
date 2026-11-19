# File node images

Build from the repository root:

```bash
bash examples/file-nodes/build.sh
```

To use an existing Ubuntu-compatible base image, set `BASE_IMAGE`:

```bash
BASE_IMAGE=harbor.lightwheel.net/cloud/ubuntu-base:v0.0.1 bash examples/file-nodes/build.sh
```

The agent mounts inputs read-only at `/data/inputs` and writable outputs at
`/data/outputs`. Paths below are relative to those mounts. The `command` array
supplies arguments to each image's entrypoint. Match filenames to the node's
`inputs[].file_name` and `outputs[].file_name` declarations; the container does
not receive those declarations automatically.

| Image | Command arguments | Behavior |
| --- | --- | --- |
| `rayflow-file-generate:local` | `OUTPUT [OUTPUT ...]` | Generate each named output with repeatable zero-filled content. |
| `rayflow-file-rename:local` | `INPUT OUTPUT [INPUT OUTPUT ...]` | Copy each read-only input to its new output name, preserving bytes. |
| `rayflow-file-verify:local` | `INPUT INPUT` | Compare exactly two files byte for byte. No output file. |

Generate node `settings.config` (12 MiB per file to exercise large artifact transfers):

```json
{
  "image": "rayflow-file-generate:local",
  "command": ["original.bin"],
  "env": ["RAYFLOW_FILE_SIZE_BYTES=12582912"]
}
```

Declare `original.bin` as a File output with `multiple: false`. The default
size is 1 MiB per file. All generated files of equal size have identical content.
Nested output paths are supported.

Rename node `settings.config`:

```json
{
  "image": "rayflow-file-rename:local",
  "command": ["original.bin", "renamed.bin"]
}
```

Declare `original.bin` as the input and `renamed.bin` as the output.

Verify node `settings.config`:

```json
{
  "image": "rayflow-file-verify:local",
  "command": ["original.bin", "renamed.bin"]
}
```

Declare both files as required inputs, referencing their respective producers.
The verifier exits 0 for identical files and 1 for different files. Missing files
and invalid arguments exit 2. The images require no network access when running.
Build them on each Docker host that runs an agent, or publish them to a registry
and use those image names in the graph.

Run the Docker smoke test after building:

```bash
python3 examples/file-nodes/test.py
```
