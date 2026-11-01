#include "kvs_integrity.h"

#include <cstdio>
#include <cstring>

static int failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++failures;
}

static void expect_hex(const unsigned char digest[KVS_SHA256_DIGEST_SIZE],
                       const char* expected, const char* message) {
    char actual[KVS_SHA256_HEX_SIZE + 1] = {0};
    expect_true(kvs_sha256_hex_encode(digest, actual) == 0, "hex encoding succeeds");
    if (std::strcmp(actual, expected) != 0) {
        std::fprintf(stderr, "FAIL: %s\nexpected: %s\nactual:   %s\n", message, expected, actual);
        ++failures;
    }
}

int main() {
    unsigned char digest[KVS_SHA256_DIGEST_SIZE] = {0};
    unsigned char second[KVS_SHA256_DIGEST_SIZE] = {0};
    unsigned char short_record[KVS_SHA256_DIGEST_SIZE] = {0};
    unsigned char previous[KVS_SHA256_DIGEST_SIZE] = {0};
    unsigned char decoded[KVS_SHA256_DIGEST_SIZE] = {0};
    char hex[KVS_SHA256_HEX_SIZE + 1] = {0};

    expect_true(kvs_sha256_digest("abc", 3, digest) == 0, "SHA-256 digest succeeds");
    expect_hex(digest, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
               "SHA-256 matches the published abc vector");

    expect_true(kvs_sha256_chain_digest(previous, "abc", 3, digest) == 0,
                "empty-prefix chain digest succeeds");
    expect_hex(digest, "1956a9d940e04afe168416a20c404bc068f4f38a4f939a6e10046362aa02d72c",
               "chain binds the record length and content");

    expect_true(kvs_sha256_chain_digest(previous, "ab", 2, short_record) == 0,
                "short-record chain digest succeeds");
    expect_hex(short_record, "bd0e15d29838b91c62a1b626cefba1c65557c2041a452ba1ec03bbb8f87032f7",
               "chain encodes the little-endian record length");
    expect_true(!kvs_sha256_equal(digest, short_record),
                "changing the record length and content changes the chain digest");

    std::memset(previous, 1, sizeof(previous));
    expect_true(kvs_sha256_chain_digest(previous, "abc", 3, second) == 0,
                "non-empty-prefix chain digest succeeds");
    expect_hex(second, "f7d77ec04e7238697eb26e03c3d08bfcee0e723758ea137bc90ba944eefb2845",
               "chain binds the previous digest");
    expect_true(!kvs_sha256_equal(digest, second), "different digests compare unequal");

    expect_true(kvs_sha256_hex_encode(digest, hex) == 0, "digest hex encoding succeeds");
    expect_true(kvs_sha256_hex_decode(hex, decoded) == 0, "digest hex decoding succeeds");
    expect_true(kvs_sha256_equal(digest, decoded), "hex round trip preserves the digest");
    expect_true(kvs_sha256_hex_decode("not-hex", decoded) != 0,
                "invalid digest hex is rejected");

    if (failures != 0) {
        return 1;
    }
    std::printf("integrity tests passed\n");
    return 0;
}
