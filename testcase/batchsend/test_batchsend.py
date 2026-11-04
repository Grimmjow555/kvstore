#!/usr/bin/env python3

import re
import socket
import struct
import subprocess
import sys
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "build" / "testcase" / "batchsend" / "batchsend"
ENGINE_COMMANDS = {
    "rbtree": b"RSET",
    "array": b"SET",
    "hash": b"HSET",
    "skiptable": b"SSET",
}
BATCH_SIZES = (10, 20, 40, 80, 160)
EXPECTED_COUNTS = {
    10: {b"RSET": 3, b"SET": 3, b"HSET": 2, b"SSET": 2},
    20: {b"RSET": 5, b"SET": 5, b"HSET": 5, b"SSET": 5},
    40: {b"RSET": 10, b"SET": 10, b"HSET": 10, b"SSET": 10},
    80: {b"RSET": 20, b"SET": 20, b"HSET": 20, b"SSET": 20},
    160: {b"RSET": 40, b"SET": 40, b"HSET": 40, b"SSET": 40},
}


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


def read_line(payload, offset):
    end = payload.find(b"\r\n", offset)
    if end < 0:
        raise AssertionError(f"missing CRLF at payload offset {offset}")
    return payload[offset:end], end + 2


def parse_commands(payload, expected_count):
    commands = []
    offset = 0
    while offset < len(payload):
        array_line, offset = read_line(payload, offset)
        if not array_line.startswith(b"*"):
            raise AssertionError(f"expected RESP array, got {array_line!r}")
        array_len = int(array_line[1:])
        items = []
        for _ in range(array_len):
            bulk_line, offset = read_line(payload, offset)
            if not bulk_line.startswith(b"$"):
                raise AssertionError(f"expected RESP bulk string, got {bulk_line!r}")
            item_len = int(bulk_line[1:])
            end = offset + item_len
            if end + 2 > len(payload) or payload[end:end + 2] != b"\r\n":
                raise AssertionError("truncated RESP bulk string")
            items.append(payload[offset:end])
            offset = end + 2
        commands.append(items)

    if len(commands) != expected_count:
        raise AssertionError(
            f"expected {expected_count} commands in one frame, got {len(commands)}"
        )
    return commands


def run_case(batch_size):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(5)
    port = listener.getsockname()[1]
    server_error = []

    def serve():
        try:
            conn, _ = listener.accept()
            with conn:
                header = recv_exact(conn, 4)
                payload_len = struct.unpack("!I", header)[0]
                payload = recv_exact(conn, payload_len)
                commands = parse_commands(payload, batch_size)

                actual_counts = {
                    command: sum(1 for item in commands if item[0] == command)
                    for command in ENGINE_COMMANDS.values()
                }
                if actual_counts != EXPECTED_COUNTS[batch_size]:
                    raise AssertionError(
                        f"engine distribution mismatch: {actual_counts!r} "
                        f"!= {EXPECTED_COUNTS[batch_size]!r}"
                    )

                next_index = {command: 0 for command in ENGINE_COMMANDS.values()}
                for command in commands:
                    if len(command) != 3:
                        raise AssertionError(f"command must have 3 items: {command!r}")
                    engine_command, key, value = command
                    if engine_command not in next_index:
                        raise AssertionError(f"unknown engine command: {engine_command!r}")
                    index = next_index[engine_command]
                    expected = [
                        engine_command,
                        f"Teacher{index}".encode(),
                        f"King{index}".encode(),
                    ]
                    if command != expected:
                        raise AssertionError(
                            f"{engine_command!r} argument {index} mismatch: "
                            f"{command!r} != {expected!r}"
                        )
                    next_index[engine_command] += 1

                response = b"+OK\r\n" * batch_size
                conn.sendall(struct.pack("!I", len(response)) + response)
        except Exception as exc:
            server_error.append(exc)

    server_thread = threading.Thread(target=serve)
    server_thread.start()
    try:
        result = subprocess.run(
            [str(BINARY), "127.0.0.1", str(port), str(batch_size)],
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        server_thread.join(timeout=6)
        listener.close()

    if server_thread.is_alive():
        raise AssertionError("mock server did not finish")
    if result.returncode != 0:
        raise AssertionError(
            f"client exited with {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if server_error:
        raise server_error[0]

    performance = re.search(
        r"time_used: ([0-9]+) ms, qps: ([0-9]+(?:\.[0-9]+)?), "
        r"avg: ([0-9]+\.[0-9]+) ms/cmd",
        result.stdout,
    )
    if not performance:
        raise AssertionError(f"missing performance summary:\n{result.stdout}")
    if float(performance.group(2)) <= 0 or float(performance.group(3)) <= 0:
        raise AssertionError(f"performance values must be positive:\n{result.stdout}")

    print(f"PASS batch={batch_size} distribution={EXPECTED_COUNTS[batch_size]!r}")


def main():
    if not BINARY.is_file():
        print(f"missing binary: {BINARY}", file=sys.stderr)
        return 1
    try:
        for batch_size in BATCH_SIZES:
            run_case(batch_size)
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("All batchsend cases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
