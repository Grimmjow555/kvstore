#include "kvs_config.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <mutex>
#include <string>
#include <strings.h>
#include <unistd.h>

static kvs_config_t g_config;
static std::mutex g_log_mutex;

static const char* kvs_log_level_name(int level) {
    switch (level) {
    case KVS_LOG_DEBUG:
        return "DEBUG";
    case KVS_LOG_INFO:
        return "INFO";
    case KVS_LOG_WARN:
        return "WARN";
    case KVS_LOG_ERROR:
        return "ERROR";
    default:
        return "OFF";
    }
}

static void kvs_log_timestamp(char* buffer, size_t size) {
    time_t now = time(nullptr);
    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    localtime_r(&now, &tm_value);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", &tm_value);
}

static std::string trim(const std::string& text) {
    size_t begin = 0;
    size_t end = text.size();

    while (begin < end && std::isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }

    return text.substr(begin, end - begin);
}

static std::string to_lower(const std::string& text) {
    std::string out = text;
    for (size_t i = 0; i < out.size(); ++i) {
        out[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(out[i])));
    }
    return out;
}

static bool parse_on_off(const std::string& value, int* out) {
    if (out == nullptr) {
        return false;
    }

    std::string lower = to_lower(trim(value));
    if (lower == "on" || lower == "true" || lower == "yes" || lower == "1" ||
        lower == "enabled") {
        *out = 1;
        return true;
    }
    if (lower == "off" || lower == "false" || lower == "no" || lower == "0" ||
        lower == "disabled") {
        *out = 0;
        return true;
    }

    return false;
}

static bool parse_port(const std::string& value, int* out) {
    if (out == nullptr || value.empty()) {
        return false;
    }

    std::string text = trim(value);
    char* end = nullptr;
    long parsed = std::strtol(text.c_str(), &end, 10);
    if (end == text.c_str() || *end != '\0' || parsed <= 0 || parsed > 65535) {
        return false;
    }

    *out = static_cast<int>(parsed);
    return true;
}

static bool parse_log_level(const std::string& value, int* out) {
    if (out == nullptr) {
        return false;
    }

    std::string lower = to_lower(trim(value));
    if (lower == "debug" || lower == "trace" || lower == "0") {
        *out = KVS_LOG_DEBUG;
    } else if (lower == "info" || lower == "information" || lower == "1") {
        *out = KVS_LOG_INFO;
    } else if (lower == "warn" || lower == "warning" || lower == "2") {
        *out = KVS_LOG_WARN;
    } else if (lower == "error" || lower == "3") {
        *out = KVS_LOG_ERROR;
    } else if (lower == "off" || lower == "none" || lower == "silent" || lower == "4") {
        *out = KVS_LOG_OFF;
    } else {
        return false;
    }

    return true;
}

static bool parse_role(const std::string& value, int* out) {
    if (out == nullptr) {
        return false;
    }

    std::string lower = to_lower(trim(value));
    if (lower == "master" || lower == "primary" || lower == "0") {
        *out = 0;
    } else if (lower == "replica" || lower == "slave" || lower == "secondary" ||
               lower == "1") {
        *out = 1;
    } else {
        return false;
    }

    return true;
}

static bool parse_persistence_mode(const std::string& value, int* rdb, int* aof) {
    if (rdb == nullptr || aof == nullptr) {
        return false;
    }

    std::string lower = to_lower(trim(value));
    if (lower == "none" || lower == "off" || lower == "disabled") {
        *rdb = 0;
        *aof = 0;
        return true;
    }
    if (lower == "rdb") {
        *rdb = 1;
        *aof = 0;
        return true;
    }
    if (lower == "aof") {
        *rdb = 0;
        *aof = 1;
        return true;
    }
    if (lower == "both" || lower == "all" || lower == "rdb,aof" || lower == "aof,rdb") {
        *rdb = 1;
        *aof = 1;
        return true;
    }

    // 兼容逗号分隔写法，例如 "rdb, aof"。
    bool rdb_on = false;
    bool aof_on = false;
    size_t begin = 0;
    while (begin <= lower.size()) {
        size_t comma = lower.find(',', begin);
        size_t length = comma == std::string::npos ? std::string::npos : comma - begin;
        std::string part = to_lower(trim(lower.substr(begin, length)));
        if (part == "rdb") {
            rdb_on = true;
        } else if (part == "aof") {
            aof_on = true;
        } else if (!part.empty()) {
            return false;
        }

        if (comma == std::string::npos) {
            break;
        }
        begin = comma + 1;
    }

    if (!rdb_on && !aof_on) {
        return false;
    }

    *rdb = rdb_on ? 1 : 0;
    *aof = aof_on ? 1 : 0;
    return true;
}

static void set_bind_ip(const std::string& value) {
    std::string ip = trim(value);
    if (ip.empty() || ip == "*") {
        ip = "0.0.0.0";
    }
    snprintf(g_config.bind_ip, sizeof(g_config.bind_ip), "%s", ip.c_str());
}

static void set_master_ip(const std::string& value) {
    std::string ip = trim(value);
    snprintf(g_config.master_ip, sizeof(g_config.master_ip), "%s", ip.c_str());
}

static int apply_config_key(const std::string& raw_key, const std::string& raw_value) {
    std::string key = to_lower(trim(raw_key));
    std::string value = trim(raw_value);

    if (key.empty()) {
        return -1;
    }

    if (key == "bind" || key == "bind_ip" || key == "ip" || key == "listen" ||
        key == "address") {
        set_bind_ip(value);
        return 0;
    }

    if (key == "port" || key == "listen_port") {
        int parsed = 0;
        if (!parse_port(value, &parsed)) {
            return -1;
        }
        g_config.port = parsed;
        return 0;
    }

    if (key == "log_level" || key == "loglevel" || key == "log") {
        int parsed = 0;
        if (!parse_log_level(value, &parsed)) {
            return -1;
        }
        g_config.log_level = parsed;
        return 0;
    }

    if (key == "role" || key == "mode" || key == "replication_role" ||
        key == "replication_mode" || key == "master_slave_mode") {
        int parsed = 0;
        if (!parse_role(value, &parsed)) {
            return -1;
        }
        g_config.role = parsed;
        return 0;
    }

    if (key == "master_ip" || key == "master_host" || key == "replica_of_ip") {
        set_master_ip(value);
        return 0;
    }

    if (key == "master_port" || key == "replica_of_port") {
        int parsed = 0;
        if (!parse_port(value, &parsed)) {
            return -1;
        }
        g_config.master_port = parsed;
        return 0;
    }

    if (key == "persistence_mode" || key == "persistence") {
        int rdb = 0;
        int aof = 0;
        if (!parse_persistence_mode(value, &rdb, &aof)) {
            return -1;
        }
        g_config.rdb_enabled = rdb;
        g_config.aof_enabled = aof;
        return 0;
    }

    if (key == "rdb" || key == "snapshot") {
        int parsed = 0;
        if (!parse_on_off(value, &parsed)) {
            return -1;
        }
        g_config.rdb_enabled = parsed;
        return 0;
    }

    if (key == "aof" || key == "appendonly") {
        int parsed = 0;
        if (!parse_on_off(value, &parsed)) {
            return -1;
        }
        g_config.aof_enabled = parsed;
        return 0;
    }

    // 未知键不视为致命错误，便于后续扩展配置项。
    return 0;
}

void kvs_config_set_defaults(void) {
    memset(&g_config, 0, sizeof(g_config));
    snprintf(g_config.bind_ip, sizeof(g_config.bind_ip), "%s", "0.0.0.0");
    g_config.port = 9999;
    g_config.log_level = KVS_LOG_INFO;
    g_config.role = 0;
    g_config.master_ip[0] = '\0';
    g_config.master_port = 0;
    // 默认关闭 RDB 与 AOF，需要时通过配置文件或命令行开关显式开启。
    g_config.rdb_enabled = 0;
    g_config.aof_enabled = 0;
}

int kvs_config_load(const char* filename) {
    if (filename == nullptr || filename[0] == '\0') {
        return -1;
    }

    std::ifstream input(filename);
    if (!input.is_open()) {
        return -1;
    }

    std::string line;
    int line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;

        std::string text = trim(line);
        if (text.empty() || text[0] == '#' || text[0] == ';') {
            continue;
        }

        // 去掉行尾注释：只去掉空格/制表符之后的 # 或 ;，避免误伤值中的字符。
        for (size_t i = 0; i < text.size(); ++i) {
            if ((text[i] == '#' || text[i] == ';') &&
                (i == 0 || std::isspace(static_cast<unsigned char>(text[i - 1])))) {
                text = text.substr(0, i);
                text = trim(text);
                break;
            }
        }
        if (text.empty()) {
            continue;
        }

        size_t separator = text.find('=');
        if (separator == std::string::npos) {
            separator = text.find_first_of(" \t");
        }
        if (separator == std::string::npos) {
            fprintf(stderr, "Invalid config line %d: %s\n", line_number, text.c_str());
            return -1;
        }

        std::string key = trim(text.substr(0, separator));
        std::string value = trim(text.substr(separator + 1));
        if (apply_config_key(key, value) != 0) {
            fprintf(stderr, "Invalid value for '%s' at config line %d\n", key.c_str(),
                    line_number);
            return -1;
        }
    }

    return 0;
}

int kvs_config_load_default(void) {
    const char* candidates[] = {"kvstore.conf", "../kvstore.conf"};
    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); ++i) {
        if (access(candidates[i], F_OK) == 0) {
            return kvs_config_load(candidates[i]);
        }
    }
    return 0;
}

static int get_option_value(char** args, int argc, int index, const char* option,
                            const char** value, int* consumed) {
    if (value == nullptr || consumed == nullptr || option == nullptr) {
        return -1;
    }

    size_t option_len = strlen(option);
    if (strncmp(args[index], option, option_len) == 0 && args[index][option_len] == '=') {
        *value = args[index] + option_len + 1;
        *consumed = 1;
        return 0;
    }

    if (strcmp(args[index], option) == 0) {
        if (index + 1 >= argc) {
            return -1;
        }
        *value = args[index + 1];
        *consumed = 2;
        return 0;
    }

    return 1;
}

int kvs_config_parse_switches(int* argc, char*** argv) {
    if (argc == nullptr || argv == nullptr || *argv == nullptr) {
        return -1;
    }

    char** args = *argv;
    int input_count = *argc;

    // 先确认是否显式指定了 --config；若没有，则加载默认配置文件。
    const char* explicit_config = nullptr;
    for (int i = 1; i < input_count; ++i) {
        int consumed = 0;
        if (get_option_value(args, input_count, i, "--config", &explicit_config, &consumed) ==
            0) {
            break;
        }
    }

    if (explicit_config != nullptr) {
        if (kvs_config_load(explicit_config) != 0) {
            fprintf(stderr, "Failed to load config file: %s\n", explicit_config);
            return -1;
        }
    } else if (kvs_config_load_default() != 0) {
        fprintf(stderr, "Failed to load default config file.\n");
        return -1;
    }

    int output_index = 1;
    for (int i = 1; i < input_count;) {
        const char* value = nullptr;
        int consumed = 0;
        int match = 0;

        if (get_option_value(args, input_count, i, "--config", &value, &consumed) == 0) {
            i += consumed;
            continue;
        }

        if (get_option_value(args, input_count, i, "--bind", &value, &consumed) == 0) {
            set_bind_ip(value == nullptr ? "" : value);
            match = 1;
        } else if (get_option_value(args, input_count, i, "--port", &value, &consumed) == 0) {
            if (!parse_port(value == nullptr ? "" : value, &g_config.port)) {
                return -1;
            }
            match = 1;
        } else if (get_option_value(args, input_count, i, "--log-level", &value, &consumed) ==
                   0) {
            if (!parse_log_level(value == nullptr ? "" : value, &g_config.log_level)) {
                return -1;
            }
            match = 1;
        } else if (get_option_value(args, input_count, i, "--role", &value, &consumed) == 0) {
            if (!parse_role(value == nullptr ? "" : value, &g_config.role)) {
                return -1;
            }
            match = 1;
        } else if (get_option_value(args, input_count, i, "--master-ip", &value, &consumed) ==
                   0) {
            set_master_ip(value == nullptr ? "" : value);
            match = 1;
        } else if (get_option_value(args, input_count, i, "--master-port", &value, &consumed) ==
                   0) {
            if (!parse_port(value == nullptr ? "" : value, &g_config.master_port)) {
                return -1;
            }
            match = 1;
        } else if (get_option_value(args, input_count, i, "--persistence-mode", &value,
                                    &consumed) == 0) {
            int rdb = 0;
            int aof = 0;
            if (!parse_persistence_mode(value == nullptr ? "" : value, &rdb, &aof)) {
                return -1;
            }
            g_config.rdb_enabled = rdb;
            g_config.aof_enabled = aof;
            match = 1;
        } else if (get_option_value(args, input_count, i, "--rdb", &value, &consumed) == 0) {
            if (!parse_on_off(value == nullptr ? "" : value, &g_config.rdb_enabled)) {
                return -1;
            }
            match = 1;
        } else if (get_option_value(args, input_count, i, "--aof", &value, &consumed) == 0) {
            if (!parse_on_off(value == nullptr ? "" : value, &g_config.aof_enabled)) {
                return -1;
            }
            match = 1;
        }

        if (match) {
            i += consumed;
            continue;
        }

        if (args[i] != nullptr && strncmp(args[i], "--", 2) == 0) {
            fprintf(stderr, "Unknown option: %s\n", args[i]);
            return -1;
        }

        args[output_index++] = args[i++];
    }

    args[output_index] = nullptr;
    *argc = output_index;
    return 0;
}

const char* kvs_config_bind_ip(void) { return g_config.bind_ip; }

int kvs_config_port(void) { return g_config.port; }

int kvs_config_log_level(void) { return g_config.log_level; }

int kvs_config_role(void) { return g_config.role; }

const char* kvs_config_master_ip(void) { return g_config.master_ip; }

int kvs_config_master_port(void) { return g_config.master_port; }

int kvs_config_rdb_enabled(void) { return g_config.rdb_enabled; }

int kvs_config_aof_enabled(void) { return g_config.aof_enabled; }

void kvs_log(int level, const char* fmt, ...) {
    if (fmt == nullptr) {
        return;
    }
    if (level < KVS_LOG_DEBUG || level >= KVS_LOG_OFF || level < g_config.log_level) {
        return;
    }

    char timestamp[32] = {0};
    kvs_log_timestamp(timestamp, sizeof(timestamp));

    std::lock_guard<std::mutex> lock(g_log_mutex);
    fprintf(stderr, "[%s][%s] ", timestamp, kvs_log_level_name(level));
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
}
