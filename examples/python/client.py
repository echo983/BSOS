#!/usr/bin/env python3
"""
Official Python client example for BSOS (Bare Space Object Storage).

Requires:
    pip install grpcio grpcio-tools xxhash
    ./generate_stubs.sh
"""

import sys
import time
from typing import Generator, Optional, Tuple

try:
    import grpc
    import xxhash
    import bsos_pb2
    import bsos_pb2_grpc
except ImportError as e:
    sys.exit(
        f"Missing dependencies ({e}). Please install requirements:\n"
        f"  pip install -r requirements.txt\n"
        f"  ./generate_stubs.sh"
    )

CHUNK_SIZE = 1024 * 1024  # 1 MiB


class BSOSClient:
    """BSOSClient interacts with a BSOS daemon over gRPC."""

    def __init__(self, target: str = "127.0.0.1:9090"):
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = bsos_pb2_grpc.BSOSStub(self.channel)

    def close(self):
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def compute_fid(data: bytes) -> int:
        """Compute the 64-bit content address using XXH3 64-bit."""
        return xxhash.xxh3_64_intdigest(data)

    def health(self) -> bool:
        """Check daemon health."""
        resp = self.stub.Health(bsos_pb2.Empty())
        return resp.ok

    def bonnie(self) -> int:
        """Query Bonnie target capacity ch_d_pow2."""
        resp = self.stub.Bonnie(bsos_pb2.Empty())
        return resp.ch_d_pow2

    def head(self, fid: int) -> int:
        """Get object size in bytes."""
        resp = self.stub.Head(bsos_pb2.HeadRequest(fid=fid))
        return resp.size

    def put(self, fid: int, data: bytes, alias_for: int = 0):
        """
        Stream a payload to BSOS.
        Step 1: PutHeader
        Step 2: Stream chunk messages
        """
        def request_generator() -> Generator[bsos_pb2.PutRequest, None, None]:
            # Step 1: Send Header
            header = bsos_pb2.PutHeader(
                fid=fid,
                total_size=len(data),
                alias_for=alias_for,
            )
            yield bsos_pb2.PutRequest(header=header)

            # Step 2: Send Chunks
            for offset in range(0, len(data), CHUNK_SIZE):
                chunk = data[offset : offset + CHUNK_SIZE]
                yield bsos_pb2.PutRequest(chunk=chunk)

        self.stub.Put(request_generator())

    def put_with_jump_retry(self, data: bytes, max_jumps: int = 255) -> Tuple[int, int, int]:
        """
        Store content with automatic collision resolution.
        Returns: (logical_fid, physical_target_fid, jumps_taken)
        """
        fid = self.compute_fid(data)
        try:
            self.put(fid, data)
            return fid, fid, 0
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.ALREADY_EXISTS:
                raise

        # Collision encountered: execute one-hop jump retries
        for jump_code in range(1, max_jumps + 1):
            jump_data = data + bytes([jump_code])
            jump_fid = self.compute_fid(jump_data)
            try:
                self.put(jump_fid, jump_data, alias_for=fid)
                return fid, jump_fid, jump_code
            except grpc.RpcError as err:
                if err.code() != grpc.StatusCode.ALREADY_EXISTS:
                    raise

        raise RuntimeError(f"Jump retries exhausted after {max_jumps} attempts")

    def get(self, fid: int, range_start: int = 0, range_end: int = 0) -> bytes:
        """Retrieve full object or specified byte range."""
        req = bsos_pb2.GetRequest(
            fid=fid,
            has_range=(range_start > 0 or range_end > 0),
            range_start=range_start,
            range_end=range_end,
        )
        stream = self.stub.Get(req)
        chunks = []
        for resp in stream:
            if resp.data:
                chunks.append(resp.data)
        return b"".join(chunks)


def main():
    target = "127.0.0.1:9090"
    if len(sys.argv) > 1:
        target = sys.argv[1]

    with BSOSClient(target) as client:
        print(f"[+] Connected to BSOS daemon at {target}")
        ok = client.health()
        print(f"[+] Health check: {'OK' if ok else 'DEGRADED'}")

        chd = client.bonnie()
        print(f"[+] Bonnie ch_d_pow2: {chd} (target object max: {1 << chd} bytes)")

        # Upload object
        payload = b"Hello from Python client! Stored in Bare Space Object Storage."
        fid, target_fid, jumps = client.put_with_jump_retry(payload)
        print(f"[+] Put complete: fid=0x{fid:016x}, target=0x{target_fid:016x}, jumps={jumps}")

        # Head
        size = client.head(fid)
        print(f"[+] Head size: {size} bytes")

        # Get
        retrieved = client.get(fid)
        assert retrieved == payload, "Retrieved data does not match!"
        print(f"[+] Get verified: {retrieved.decode('utf-8')}")

        # Range Get
        part = client.get(fid, range_start=11, range_end=24)
        print(f"[+] Range [11..24): {part.decode('utf-8')}")


if __name__ == "__main__":
    main()
