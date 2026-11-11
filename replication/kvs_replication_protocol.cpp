#include "kvs_replication_protocol.h"

#include <stdio.h>
#include <string.h>

namespace {

const int kMaxHandshakeArgs = 3;
const int kMaxHandshakeArgSize = 64;

int parse_resp_array(const char* payload, int length, char args[][kMaxHandshakeArgSize],
                     int max_args, int* out_argc) {
    if (payload == NULL || length <= 0 || args == NULL || out_argc == NULL) {
        return -1;
    }

    int offset = 0;
    int count = 0;
    if (length < 4 || payload[offset++] != '*') {
        return -1;
    }
    int array_count = 0;
    while (offset < length && payload[offset] >= '0' && payload[offset] <= '9') {
        array_count = array_count * 10 + (payload[offset] - '0');
        ++offset;
    }
    if (offset + 2 > length || payload[offset++] != '\r' || payload[offset++] != '\n' ||
        array_count <= 0 || array_count > max_args) {
        return -1;
    }

    for (int i = 0; i < array_count; ++i) {
        if (offset >= length || payload[offset++] != '$') {
            return -1;
        }
        int arg_length = 0;
        int digits = 0;
        while (offset < length && payload[offset] >= '0' && payload[offset] <= '9') {
            arg_length = arg_length * 10 + (payload[offset] - '0');
            ++offset;
            ++digits;
        }
        if (digits == 0 || arg_length <= 0 || arg_length >= kMaxHandshakeArgSize ||
            offset + 2 > length || payload[offset++] != '\r' || payload[offset++] != '\n' ||
            offset + arg_length + 2 > length) {
            return -1;
        }
        memcpy(args[count], payload + offset, (size_t)arg_length);
        args[count][arg_length] = '\0';
        ++count;
        offset += arg_length;
        if (payload[offset++] != '\r' || payload[offset++] != '\n') {
            return -1;
        }
    }

    if (offset != length) {
        return -1;
    }
    *out_argc = count;
    return 0;
}

} // namespace

int kvs_repl_format_session_id(unsigned long long value, char* out, size_t out_size) {
    if (out == NULL || out_size < KVS_REPL_SESSION_ID_SIZE + 1) {
        return -1;
    }
    int written = snprintf(out, out_size, "%016llx", value);
    return written == KVS_REPL_SESSION_ID_SIZE ? 0 : -1;
}

int kvs_repl_validate_session_id(const char* session_id) {
    if (session_id == NULL || strlen(session_id) != KVS_REPL_SESSION_ID_SIZE) {
        return 0;
    }
    for (size_t i = 0; i < KVS_REPL_SESSION_ID_SIZE; ++i) {
        char c = session_id[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return 0;
        }
    }
    return 1;
}

int kvs_repl_build_resp_array(const char* const argv[], int argc, char* out, size_t out_size,
                              size_t* out_len) {
    if (argv == NULL || argc <= 0 || out == NULL || out_size == 0 || out_len == NULL) {
        return -1;
    }
    int offset = snprintf(out, out_size, "*%d\r\n", argc);
    if (offset <= 0 || (size_t)offset >= out_size) {
        return -1;
    }
    for (int i = 0; i < argc; ++i) {
        if (argv[i] == NULL) {
            return -1;
        }
        int written = snprintf(out + offset, out_size - (size_t)offset, "$%zu\r\n%s\r\n",
                               strlen(argv[i]), argv[i]);
        if (written <= 0 || (size_t)offset + (size_t)written >= out_size) {
            return -1;
        }
        offset += written;
    }
    *out_len = (size_t)offset;
    return 0;
}

int kvs_repl_parse_ok_session(const char* response, int length, char* session_id,
                              size_t session_id_size) {
    if (response == NULL || session_id == NULL || session_id_size < KVS_REPL_SESSION_ID_SIZE + 1 ||
        length != 4 + KVS_REPL_SESSION_ID_SIZE + 2 || memcmp(response, "+OK ", 4) != 0 ||
        response[4 + KVS_REPL_SESSION_ID_SIZE] != '\r' ||
        response[5 + KVS_REPL_SESSION_ID_SIZE] != '\n') {
        return -1;
    }
    memcpy(session_id, response + 4, KVS_REPL_SESSION_ID_SIZE);
    session_id[KVS_REPL_SESSION_ID_SIZE] = '\0';
    if (!kvs_repl_validate_session_id(session_id)) {
        session_id[0] = '\0';
        return -1;
    }
    return 0;
}

int kvs_repl_build_ebpf_handshake(char* out, size_t out_size, size_t* out_len) {
    const char* argv[] = {"EBPF", KVS_REPL_PROTOCOL_VERSION};
    return kvs_repl_build_resp_array(argv, 2, out, out_size, out_len);
}

int kvs_repl_parse_ebpf_handshake(const char* payload, int length) {
    char args[kMaxHandshakeArgs][kMaxHandshakeArgSize];
    int argc = 0;
    if (parse_resp_array(payload, length, args, kMaxHandshakeArgs, &argc) != 0 || argc != 2 ||
        strcmp(args[0], "EBPF") != 0 || strcmp(args[1], KVS_REPL_PROTOCOL_VERSION) != 0) {
        return -1;
    }
    return 0;
}

int kvs_repl_build_repl_inc(const char* session_id, char* out, size_t out_size, size_t* out_len) {
    if (!kvs_repl_validate_session_id(session_id)) {
        return -1;
    }
    const char* argv[] = {"REPL_INC", KVS_REPL_PROTOCOL_VERSION, session_id};
    return kvs_repl_build_resp_array(argv, 3, out, out_size, out_len);
}

int kvs_repl_parse_repl_inc(const char* payload, int length, char* session_id,
                            size_t session_id_size) {
    char args[kMaxHandshakeArgs][kMaxHandshakeArgSize];
    int argc = 0;
    if (session_id == NULL || session_id_size < KVS_REPL_SESSION_ID_SIZE + 1 ||
        parse_resp_array(payload, length, args, kMaxHandshakeArgs, &argc) != 0 || argc != 3 ||
        strcmp(args[0], "REPL_INC") != 0 ||
        strcmp(args[1], KVS_REPL_PROTOCOL_VERSION) != 0 ||
        !kvs_repl_validate_session_id(args[2])) {
        return -1;
    }
    snprintf(session_id, session_id_size, "%s", args[2]);
    return 0;
}
