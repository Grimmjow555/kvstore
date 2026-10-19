#include "kvs_integrity.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <cstdint>

extern "C" {

int kvs_sha256_digest(const void* data, size_t len,
                      unsigned char out[KVS_SHA256_DIGEST_SIZE]) {
    if (out == nullptr || (data == nullptr && len != 0)) {
        return -1;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr) {
        return -1;
    }

    unsigned int digest_len = 0;
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1 &&
             (len == 0 || EVP_DigestUpdate(ctx, data, len) == 1) &&
             EVP_DigestFinal_ex(ctx, out, &digest_len) == 1 &&
             digest_len == KVS_SHA256_DIGEST_SIZE;
    EVP_MD_CTX_free(ctx);
    return ok ? 0 : -1;
}

int kvs_sha256_chain_digest(const unsigned char previous[KVS_SHA256_DIGEST_SIZE],
                            const void* record, size_t record_len,
                            unsigned char out[KVS_SHA256_DIGEST_SIZE]) {
    if (previous == nullptr || out == nullptr || (record == nullptr && record_len != 0)) {
        return -1;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr) {
        return -1;
    }

    unsigned char length_bytes[sizeof(uint64_t)];
    uint64_t length = (uint64_t)record_len;
    for (size_t i = 0; i < sizeof(length_bytes); ++i) {
        length_bytes[i] = (unsigned char)(length >> (i * 8));
    }

    unsigned int digest_len = 0;
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1 &&
             EVP_DigestUpdate(ctx, previous, KVS_SHA256_DIGEST_SIZE) == 1 &&
             EVP_DigestUpdate(ctx, length_bytes, sizeof(length_bytes)) == 1 &&
             (record_len == 0 || EVP_DigestUpdate(ctx, record, record_len) == 1) &&
             EVP_DigestFinal_ex(ctx, out, &digest_len) == 1 &&
             digest_len == KVS_SHA256_DIGEST_SIZE;
    EVP_MD_CTX_free(ctx);
    return ok ? 0 : -1;
}

int kvs_sha256_hex_encode(const unsigned char digest[KVS_SHA256_DIGEST_SIZE],
                          char out[KVS_SHA256_HEX_SIZE + 1]) {
    static const char kHex[] = "0123456789abcdef";
    if (digest == nullptr || out == nullptr) {
        return -1;
    }

    for (size_t i = 0; i < KVS_SHA256_DIGEST_SIZE; ++i) {
        out[i * 2] = kHex[digest[i] >> 4];
        out[i * 2 + 1] = kHex[digest[i] & 0x0f];
    }
    out[KVS_SHA256_HEX_SIZE] = '\0';
    return 0;
}

static int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

int kvs_sha256_hex_decode(const char* text,
                          unsigned char out[KVS_SHA256_DIGEST_SIZE]) {
    if (text == nullptr || out == nullptr) {
        return -1;
    }

    for (size_t i = 0; i < KVS_SHA256_DIGEST_SIZE; ++i) {
        int high = hex_value(text[i * 2]);
        int low = hex_value(text[i * 2 + 1]);
        if (high < 0 || low < 0) {
            return -1;
        }
        out[i] = (unsigned char)((high << 4) | low);
    }
    return text[KVS_SHA256_HEX_SIZE] == '\0' ? 0 : -1;
}

int kvs_sha256_equal(const unsigned char left[KVS_SHA256_DIGEST_SIZE],
                     const unsigned char right[KVS_SHA256_DIGEST_SIZE]) {
    if (left == nullptr || right == nullptr) {
        return 0;
    }
    return CRYPTO_memcmp(left, right, KVS_SHA256_DIGEST_SIZE) == 0 ? 1 : 0;
}

}  // extern "C"
