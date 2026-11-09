#!/usr/bin/env python3

import re
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build" / "testcase"


def recv_exact(conn, length):
    chunks = []
    received = 0
    while received < length:
        chunk = conn.recv(length - received)
        if not chunk:
            raise AssertionError("client closed the connection early")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def serve_frames(listener, frame_count, response_count, server_errors):
    try:
        conn, _ = listener.accept()
        with conn:
            conn.settimeout(30)
            response = b"+OK\r\n" * response_count
            response_frame = struct.pack("!I", len(response)) + response
            for _ in range(frame_count):
                payload_length = struct.unpack("!I", recv_exact(conn, 4))[0]
                recv_exact(conn, payload_length)
                conn.sendall(response_frame)
    except Exception as exc:
        server_errors.append(exc)


def count_send_calls(trace_path):
    count = 0
    pattern = re.compile(r"^(?:sendto|sendmsg)\(.*\)\s*=\s*([1-9][0-9]*)")
    for line in trace_path.read_text(errors="replace").splitlines():
        line = re.sub(r"^\s*[0-9]+\s+", "", line)
        if pattern.match(line):
            count += 1
    return count


def run_case(name, argv_factory, frame_count, response_count):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    port = listener.getsockname()[1]
    server_errors = []
    server_thread = threading.Thread(
        target=serve_frames,
        args=(listener, frame_count, response_count, server_errors),
    )
    server_thread.start()

    with tempfile.TemporaryDirectory(prefix="kvs-frame-send-") as temp_dir:
        trace_path = Path(temp_dir) / f"{name}.trace"
        command = [
            shutil.which("strace") or "strace",
            "-f",
            "-qq",
            "-e",
            "trace=sendto,sendmsg",
            "-o",
            str(trace_path),
            *argv_factory(port),
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        actual_calls = count_send_calls(trace_path)

    server_thread.join(timeout=35)
    listener.close()

    if server_thread.is_alive():
        raise AssertionError(f"{name}: mock server did not finish")
    if server_errors:
        raise AssertionError(f"{name}: mock server failed: {server_errors[0]}")
    if result.returncode != 0:
        raise AssertionError(
            f"{name}: client exited with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    if actual_calls != frame_count:
        raise AssertionError(
            f"{name}: expected one send syscall per frame, got {actual_calls} "
            f"send calls for {frame_count} frames"
        )
    print(f"PASS {name}: {frame_count} frame(s), {actual_calls} send call(s)")


def main():
    if shutil.which("strace") is None:
        print("missing command: strace", file=sys.stderr)
        return 1

    cases = [
        (
            "batchsend",
            lambda port: [
                str(BUILD / "batchsend" / "batchsend"),
                "127.0.0.1",
                str(port),
                "160",
            ],
            1,
            160,
        ),
        (
            "batchsend_hash",
            lambda port: [
                str(BUILD / "batchsend" / "batchsend_hash"),
                "127.0.0.1",
                str(port),
                "160",
            ],
            625,
            160,
        ),
        (
            "integrity_client",
            lambda port: [
                str(BUILD / "integrity" / "integrity_client"),
                "127.0.0.1",
                str(port),
                "PING",
            ],
            1,
            1,
        ),
    ]

    try:
        for name, argv_factory, frame_count, response_count in cases:
            run_case(name, argv_factory, frame_count, response_count)
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("All test clients send each frame in one call.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
