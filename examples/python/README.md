# BSOS Python Client Example

This directory contains a complete Python client implementation for BSOS using standard `grpcio` and `xxhash`.

## Prerequisites

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Generate the gRPC stubs from the protocol definition in `proto/bsos.proto`:

```bash
chmod +x generate_stubs.sh
./generate_stubs.sh
```

## Running the Example

Make sure the BSOS daemon (`bsosd`) is running, then run:

```bash
python3 client.py [daemon-host:port]
```

By default, it connects to `127.0.0.1:9090`.

## Features Demonstrated

1. **Deterministic Content-Addressing**: `xxhash.xxh3_64_intdigest()` computes the 64-bit FID.
2. **Streaming Protocol**: Header message sent first with `total_size`, followed by streamed 1 MiB chunk messages.
3. **Automatic Collision Resolution**: On `ALREADY_EXISTS` collision, retries with 1-byte suffix jump codes and `alias_for` header.
4. **Metadata & Range Queries**: `head()`, full `get()`, and range-sliced `get(range_start, range_end)`.
