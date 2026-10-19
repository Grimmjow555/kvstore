#!/usr/bin/env python3

import pathlib
import sys

HEADER = b"#KVSAOF02\r\n"
CHECKSUM_SIZE = 66


def parse_records(data):
    if not data.startswith(HEADER):
        raise ValueError("missing AOF v2 header")

    records = []
    offset = len(HEADER)
    while offset < len(data):
        start = offset
        if data[offset : offset + 1] != b"*":
            raise ValueError(f"record at {offset} is not RESP")

        line_end = data.index(b"\r\n", offset + 1)
        argc = int(data[offset + 1 : line_end])
        offset = line_end + 2
        for _ in range(argc):
            if data[offset : offset + 1] != b"$":
                raise ValueError(f"record at {start} has invalid argument")
            line_end = data.index(b"\r\n", offset + 1)
            length = int(data[offset + 1 : line_end])
            offset = line_end + 2 + length
            if data[offset : offset + 2] != b"\r\n":
                raise ValueError(f"record at {start} has invalid argument terminator")
            offset += 2

        command_end = offset
        if offset + CHECKSUM_SIZE > len(data):
            raise ValueError(f"record at {start} is missing its checksum")
        checksum = data[offset : offset + 64]
        if len(checksum) != 64 or data[offset + 64 : offset + CHECKSUM_SIZE] != b"\r\n":
            raise ValueError(f"record at {start} has invalid checksum framing")
        offset += CHECKSUM_SIZE
        records.append((start, command_end, offset))

    return records


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: aof_mutate.py <file> <tamper|delete|swap|truncate|legacy>")

    path = pathlib.Path(sys.argv[1])
    mode = sys.argv[2]
    data = path.read_bytes()
    records = parse_records(data)

    if mode == "tamper":
        start, command_end, _ = records[0]
        if data[command_end - 3 : command_end - 2] == b"9":
            replacement = b"8"
        else:
            replacement = b"9"
        data = data[: command_end - 3] + replacement + data[command_end - 2 :]
    elif mode == "delete":
        if len(records) < 3:
            raise ValueError("delete requires at least three records")
        start, _, end = records[1]
        data = data[:start] + data[end:]
    elif mode == "swap":
        if len(records) < 2:
            raise ValueError("swap requires at least two records")
        first_start, _, first_end = records[0]
        second_start, _, second_end = records[1]
        data = (
            data[:first_start]
            + data[second_start:second_end]
            + data[first_start:first_end]
            + data[second_end:]
        )
    elif mode == "truncate":
        data = data[:-1]
    elif mode == "legacy":
        data = data[len(HEADER) :]
    else:
        raise ValueError(f"unknown mutation: {mode}")

    path.write_bytes(data)


if __name__ == "__main__":
    main()
