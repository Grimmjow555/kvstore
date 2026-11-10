#include "kvs_agent_config.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <fstream>
#include <string>

namespace {

const long kDefaultCacheMaxBytes = 256L * 1024L * 1024L;
const long kDefaultCacheMaxRecords = 500000L;
const long kDefaultMaxReplicas = 16L;
const long kDefaultMemoryBudgetBytes = 1024L * 1024L * 1024L;

std::string trim(const std::string& text) {
    size_t begin = 0;
    size_t end = text.size();
    while (begin < end && isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }
    while (end > begin && isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }
    return text.substr(begin, end - begin);
}

std::string to_lower(const std::string& text) {
    std::string result = text;
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = static_cast<char>(tolower(static_cast<unsigned char>(result[i])));
    }
    return result;
}

std::string strip_inline_comment(const std::string& text) {
    size_t comment = text.find_first_of("#;");
    if (comment == std::string::npos) {
        return text;
    }
    return text.substr(0, comment);
}

bool copy_string(char* destination, size_t capacity, const std::string& value) {
    if (destination == NULL || capacity == 0 || value.size() >= capacity) {
        return false;
    }
    memcpy(destination, value.c_str(), value.size() + 1);
    return true;
}

bool parse_long(const std::string& value, long minimum, long maximum, long* output) {
    if (output == NULL || value.empty()) {
        return false;
    }
    errno = 0;
    char* end = NULL;
    long parsed = strtol(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed < minimum ||
        parsed > maximum) {
        return false;
    }
    *output = parsed;
    return true;
}

bool parse_mode(const std::string& value, int* output) {
    if (output == NULL) {
        return false;
    }
    std::string lower = to_lower(trim(value));
    if (lower == "uprobe") {
        *output = 0;
        return true;
    }
    if (lower == "sim") {
        *output = 1;
        return true;
    }
    return false;
}

bool parse_host_port(const std::string& text, char* ip, size_t ip_size, int* port) {
    size_t colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
        return false;
    }

    std::string ip_text = trim(text.substr(0, colon));
    std::string port_text = trim(text.substr(colon + 1));
    long parsed_port = 0;
    if (ip_text.empty() || !copy_string(ip, ip_size, ip_text) ||
        !parse_long(port_text, 1, 65535, &parsed_port)) {
        return false;
    }
    *port = static_cast<int>(parsed_port);
    return true;
}

bool apply_key(kvs_agent_config_t* config, const std::string& raw_key,
               const std::string& raw_value) {
    if (config == NULL) {
        return false;
    }

    const std::string key = to_lower(trim(raw_key));
    const std::string value = trim(raw_value);
    long parsed = 0;

    if (key == "master_ip") {
        return !value.empty() && copy_string(config->master_ip, sizeof(config->master_ip), value);
    }
    if (key == "master_port") {
        if (!parse_long(value, 1, 65535, &parsed)) {
            return false;
        }
        config->master_port = static_cast<int>(parsed);
        return true;
    }
    if (key == "listen_ip") {
        return !value.empty() && copy_string(config->listen_ip, sizeof(config->listen_ip), value);
    }
    if (key == "listen_port") {
        if (!parse_long(value, 1, 65535, &parsed)) {
            return false;
        }
        config->listen_port = static_cast<int>(parsed);
        return true;
    }
    if (key == "mode" || key == "ebpf_capture_mode") {
        return parse_mode(value, &config->mode);
    }
    if (key == "ring" || key == "ebpf_ring_file") {
        return !value.empty() && copy_string(config->ring_path, sizeof(config->ring_path), value);
    }
    if (key == "master_pid") {
        if (!parse_long(value, 1, INT_MAX, &parsed)) {
            return false;
        }
        config->master_pid = static_cast<int>(parsed);
        return true;
    }
    if (key == "master_binary") {
        return !value.empty() &&
               copy_string(config->master_binary, sizeof(config->master_binary), value);
    }
    if (key == "cache_max_bytes") {
        if (!parse_long(value, 1, LONG_MAX, &parsed)) {
            return false;
        }
        config->cache_max_bytes = parsed;
        return true;
    }
    if (key == "cache_max_records") {
        if (!parse_long(value, 1, LONG_MAX, &parsed)) {
            return false;
        }
        config->cache_max_records = parsed;
        return true;
    }
    if (key == "max_replicas") {
        if (!parse_long(value, 1, 16, &parsed)) {
            return false;
        }
        config->max_replicas = parsed;
        return true;
    }
    if (key == "memory_budget_bytes") {
        if (!parse_long(value, 1, LONG_MAX, &parsed)) {
            return false;
        }
        config->memory_budget_bytes = parsed;
        return true;
    }
    return false;
}

} // namespace

void kvs_agent_config_init(kvs_agent_config_t* config) {
    if (config == NULL) {
        return;
    }
    memset(config, 0, sizeof(*config));
    config->master_port = 9999;
    snprintf(config->listen_ip, sizeof(config->listen_ip), "%s", "0.0.0.0");
    config->listen_port = 11001;
    config->mode = 0;
    config->cache_max_bytes = kDefaultCacheMaxBytes;
    config->cache_max_records = kDefaultCacheMaxRecords;
    config->max_replicas = kDefaultMaxReplicas;
    config->memory_budget_bytes = kDefaultMemoryBudgetBytes;
}

int kvs_agent_config_load_file(kvs_agent_config_t* config, const char* path) {
    if (config == NULL || path == NULL || path[0] == '\0') {
        return -1;
    }

    std::ifstream input(path);
    if (!input.is_open()) {
        return -1;
    }

    kvs_agent_config_t parsed;
    kvs_agent_config_init(&parsed);

    std::string line;
    while (std::getline(input, line)) {
        line = trim(strip_inline_comment(line));
        if (line.empty()) {
            continue;
        }

        std::string key;
        std::string value;
        size_t separator = line.find('=');
        if (separator != std::string::npos) {
            key = trim(line.substr(0, separator));
            value = trim(line.substr(separator + 1));
        } else {
            size_t whitespace = 0;
            while (whitespace < line.size() &&
                   !isspace(static_cast<unsigned char>(line[whitespace]))) {
                ++whitespace;
            }
            key = trim(line.substr(0, whitespace));
            value = whitespace < line.size() ? trim(line.substr(whitespace)) : std::string();
        }

        if (key.empty() || value.empty() || !apply_key(&parsed, key, value)) {
            return -1;
        }
    }
    if (input.bad()) {
        return -1;
    }

    *config = parsed;
    return 0;
}

int kvs_agent_config_load_default(kvs_agent_config_t* config) {
    if (config == NULL) {
        return -1;
    }

    const char* candidates[] = {"./ebpf-agent.conf", "../ebpf-agent.conf"};
    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); ++i) {
        struct stat status;
        if (stat(candidates[i], &status) != 0) {
            continue;
        }
        return kvs_agent_config_load_file(config, candidates[i]) == 0 ? 1 : -1;
    }
    return 0;
}

int kvs_agent_config_select_path(int argc, char* const argv[], char* path, size_t path_size,
                                 int* has_explicit_path) {
    if (argc < 0 || argv == NULL || path == NULL || path_size == 0 ||
        has_explicit_path == NULL) {
        return -1;
    }

    path[0] = '\0';
    *has_explicit_path = 0;
    for (int i = 1; i < argc; ++i) {
        if (argv[i] == NULL) {
            return -1;
        }
        if (strcmp(argv[i], "--config") != 0) {
            continue;
        }
        if (*has_explicit_path || i + 1 >= argc || argv[i + 1] == NULL || argv[i + 1][0] == '\0' ||
            !copy_string(path, path_size, argv[i + 1])) {
            return -1;
        }
        *has_explicit_path = 1;
        ++i;
    }
    return 0;
}

int kvs_agent_config_parse_args(kvs_agent_config_t* config, int argc, char* const argv[]) {
    if (config == NULL || argc < 1 || argv == NULL) {
        return -1;
    }

    for (int i = 1; i < argc; ++i) {
        if (argv[i] == NULL) {
            return -1;
        }
        const char* option = argv[i];
        if (strcmp(option, "-h") == 0 || strcmp(option, "--help") == 0) {
            return 1;
        }
        if (strcmp(option, "--config") == 0) {
            if (i + 1 >= argc || argv[i + 1] == NULL || argv[i + 1][0] == '\0') {
                return -1;
            }
            ++i;
            continue;
        }

        if (i + 1 >= argc || argv[i + 1] == NULL) {
            return -1;
        }
        const std::string value = argv[++i];
        long parsed = 0;

        if (strcmp(option, "--master") == 0) {
            if (!parse_host_port(value, config->master_ip, sizeof(config->master_ip),
                                 &config->master_port)) {
                return -1;
            }
        } else if (strcmp(option, "--listen") == 0) {
            if (!parse_host_port(value, config->listen_ip, sizeof(config->listen_ip),
                                 &config->listen_port)) {
                return -1;
            }
        } else if (strcmp(option, "--mode") == 0) {
            if (!parse_mode(value, &config->mode)) {
                return -1;
            }
        } else if (strcmp(option, "--ring") == 0) {
            if (!copy_string(config->ring_path, sizeof(config->ring_path), value)) {
                return -1;
            }
        } else if (strcmp(option, "--binary") == 0) {
            if (!copy_string(config->master_binary, sizeof(config->master_binary), value)) {
                return -1;
            }
        } else if (strcmp(option, "--pid") == 0) {
            if (!parse_long(value, 1, INT_MAX, &parsed)) {
                return -1;
            }
            config->master_pid = static_cast<int>(parsed);
        } else if (strcmp(option, "--cache-max-bytes") == 0) {
            if (!parse_long(value, 1, LONG_MAX, &parsed)) {
                return -1;
            }
            config->cache_max_bytes = parsed;
        } else if (strcmp(option, "--cache-max-records") == 0) {
            if (!parse_long(value, 1, LONG_MAX, &parsed)) {
                return -1;
            }
            config->cache_max_records = parsed;
        } else if (strcmp(option, "--max-replicas") == 0) {
            if (!parse_long(value, 1, 16, &parsed)) {
                return -1;
            }
            config->max_replicas = parsed;
        } else if (strcmp(option, "--memory-budget-bytes") == 0) {
            if (!parse_long(value, 1, LONG_MAX, &parsed)) {
                return -1;
            }
            config->memory_budget_bytes = parsed;
        } else {
            return -1;
        }
    }
    return 0;
}

int kvs_agent_config_validate(const kvs_agent_config_t* config) {
    if (config == NULL || config->master_ip[0] == '\0' || config->master_port <= 0 ||
        config->master_port > 65535 || config->listen_ip[0] == '\0' ||
        config->listen_port <= 0 || config->listen_port > 65535 ||
        config->cache_max_bytes <= 0 || config->cache_max_records <= 0 ||
        config->max_replicas <= 0 || config->max_replicas > 16 ||
        config->memory_budget_bytes <= 0) {
        return -1;
    }
    if (config->mode == 1) {
        return config->ring_path[0] == '\0' ? -1 : 0;
    }
    if (config->mode == 0) {
        return config->master_pid > 0 && config->master_binary[0] != '\0' ? 0 : -1;
    }
    return -1;
}
