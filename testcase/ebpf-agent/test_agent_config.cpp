#include "kvs_agent_config.h"
#include "kvs_config.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

static void expect_int(int actual, int expected, const char* message) {
    if (actual == expected) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s (expected %d, actual %d)\n", message, expected, actual);
    ++g_failures;
}

static void expect_long(long actual, long expected, const char* message) {
    if (actual == expected) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s (expected %ld, actual %ld)\n", message, expected, actual);
    ++g_failures;
}

static void expect_string(const char* actual, const char* expected, const char* message) {
    if (actual != NULL && strcmp(actual, expected) == 0) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s (expected '%s', actual '%s')\n", message, expected,
                 actual == NULL ? "(null)" : actual);
    ++g_failures;
}

static bool write_text_file(const std::string& path, const char* contents) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return false;
    }
    size_t total = strlen(contents);
    size_t written = 0;
    while (written < total) {
        ssize_t n = write(fd, contents + written, total - written);
        if (n <= 0) {
            close(fd);
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return close(fd) == 0;
}

static std::string make_temp_file(const char* contents) {
    char path[] = "/tmp/kvs-agent-config-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) {
        return std::string();
    }
    close(fd);
    if (!write_text_file(path, contents)) {
        unlink(path);
        return std::string();
    }
    return std::string(path);
}

static void test_loads_all_agent_fields() {
    const std::string path = make_temp_file(
        "# ebpf-agent config\n"
        "master_ip 192.168.234.135\n"
        "master_port 9999\n"
        "listen_ip 192.168.234.135\n"
        "listen_port 11001\n"
        "mode sim\n"
        "ring ../data/kvstore.ebpf.ring\n"
        "master_pid 12345\n"
        "master_binary ./kvstore\n"
        "cache_max_bytes 1048576\n"
        "cache_max_records 42\n");
    expect_true(!path.empty(), "temporary config file is created");
    if (path.empty()) {
        return;
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    expect_int(kvs_agent_config_load_file(&config, path.c_str()), 0,
               "valid agent config file loads");
    expect_string(config.master_ip, "192.168.234.135", "master IP is loaded");
    expect_int(config.master_port, 9999, "master port is loaded");
    expect_string(config.listen_ip, "192.168.234.135", "listener IP is loaded");
    expect_int(config.listen_port, 11001, "listener port is loaded");
    expect_int(config.mode, 1, "sim capture mode is loaded");
    expect_string(config.ring_path, "../data/kvstore.ebpf.ring", "ring path is loaded");
    expect_int(config.master_pid, 12345, "master PID is loaded");
    expect_string(config.master_binary, "./kvstore", "master binary is loaded");
    expect_long(config.cache_max_bytes, 1048576L, "cache byte limit is loaded");
    expect_long(config.cache_max_records, 42L, "cache record limit is loaded");
    unlink(path.c_str());
}

static void test_accepts_equals_and_inline_comments() {
    const std::string path = make_temp_file(
        "master_ip=10.1.2.3 # master\n"
        "master_port = 19001\n"
        "listen_ip = 10.1.2.4 ; listener\n"
        "listen_port=20001\n"
        "mode=sim\n"
        "ring=../tmp/ring\n");
    expect_true(!path.empty(), "equals-form config file is created");
    if (path.empty()) {
        return;
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    expect_int(kvs_agent_config_load_file(&config, path.c_str()), 0,
               "equals syntax and inline comments are accepted");
    expect_string(config.master_ip, "10.1.2.3", "inline comment is removed from master IP");
    expect_string(config.listen_ip, "10.1.2.4", "semicolon comment is removed from listener IP");
    expect_int(config.master_port, 19001, "equals-form master port is loaded");
    expect_int(config.listen_port, 20001, "equals-form listener port is loaded");
    unlink(path.c_str());
}

static void test_rejects_unknown_key_and_invalid_port() {
    const std::string unknown_path = make_temp_file("mystery_key value\n");
    const std::string invalid_path = make_temp_file("master_port 70000\n");
    const std::string legacy_replica_ip = make_temp_file("replica_ip 192.168.234.129\n");
    const std::string legacy_replica_port = make_temp_file("replica_port 10001\n");
    expect_true(!unknown_path.empty() && !invalid_path.empty() && !legacy_replica_ip.empty() &&
                    !legacy_replica_port.empty(),
                "invalid fixtures are created");
    if (unknown_path.empty() || invalid_path.empty() || legacy_replica_ip.empty() ||
        legacy_replica_port.empty()) {
        return;
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    expect_true(kvs_agent_config_load_file(&config, unknown_path.c_str()) != 0,
                "unknown key rejects the config");
    expect_true(kvs_agent_config_load_file(&config, invalid_path.c_str()) != 0,
                "out-of-range port rejects the config");
    expect_true(kvs_agent_config_load_file(&config, legacy_replica_ip.c_str()) != 0,
                "legacy replica_ip is rejected");
    expect_true(kvs_agent_config_load_file(&config, legacy_replica_port.c_str()) != 0,
                "legacy replica_port is rejected");
    unlink(unknown_path.c_str());
    unlink(invalid_path.c_str());
    unlink(legacy_replica_ip.c_str());
    unlink(legacy_replica_port.c_str());
}

static void test_finds_parent_config_for_build_launch() {
    char root_template[] = "/tmp/kvs-agent-default-XXXXXX";
    char* root = mkdtemp(root_template);
    expect_true(root != NULL, "temporary default-config directory is created");
    if (root == NULL) {
        return;
    }

    std::string root_path(root);
    std::string nested = root_path + "/build";
    std::string config_path = root_path + "/ebpf-agent.conf";
    expect_true(mkdir(nested.c_str(), 0700) == 0, "launch directory is created");
    expect_true(write_text_file(config_path.c_str(),
                                "master_ip 192.168.234.135\n"
                                "master_port 9999\n"
                                "listen_ip 0.0.0.0\n"
                                "listen_port 11001\n"
                                "mode sim\n"
                                "ring ../data/kvstore.ebpf.ring\n"),
                "parent config file is written");

    char original[4096];
    expect_true(getcwd(original, sizeof(original)) != NULL, "current directory is captured");
    expect_true(chdir(nested.c_str()) == 0, "test enters build-like directory");

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    int load_rc = kvs_agent_config_load_default(&config);

    expect_true(chdir(original) == 0, "test restores its working directory");
    expect_int(load_rc, 1, "default loader finds ../ebpf-agent.conf");
    expect_string(config.master_ip, "192.168.234.135", "default config master IP is loaded");
    unlink(config_path.c_str());
    rmdir(nested.c_str());
    rmdir(root_path.c_str());
}

static void test_cli_overrides_loaded_config() {
    const std::string path = make_temp_file(
        "master_ip 192.168.234.135\n"
        "master_port 9999\n"
        "listen_ip 0.0.0.0\n"
        "listen_port 11001\n"
        "mode sim\n"
        "ring ../data/kvstore.ebpf.ring\n"
        "cache_max_records 500000\n");
    expect_true(!path.empty(), "override fixture is created");
    if (path.empty()) {
        return;
    }

    char program[] = "ebpf-agent";
    char config_option[] = "--config";
    char config_path[512];
    char master_option[] = "--master";
    char master_value[] = "10.2.3.4:19001";
    char listen_option[] = "--listen";
    char listen_value[] = "10.2.3.5:20001";
    char mode_option[] = "--mode";
    char mode_value[] = "uprobe";
    char pid_option[] = "--pid";
    char pid_value[] = "321";
    char binary_option[] = "--binary";
    char binary_value[] = "./custom-kvstore";
    char cache_option[] = "--cache-max-records";
    char cache_value[] = "77";
    strncpy(config_path, path.c_str(), sizeof(config_path) - 1);
    config_path[sizeof(config_path) - 1] = '\0';
    char* args[] = {program,          config_option, config_path,   master_option,
                    master_value,     listen_option, listen_value,  mode_option,
                    mode_value,       pid_option,     pid_value,     binary_option,
                    binary_value,     cache_option,   cache_value};
    int argc = static_cast<int>(sizeof(args) / sizeof(args[0]));

    char selected_path[512];
    int has_explicit_path = 0;
    expect_int(kvs_agent_config_select_path(argc, args, selected_path, sizeof(selected_path),
                                            &has_explicit_path),
               0, "--config path is accepted");
    expect_int(has_explicit_path, 1, "explicit config path is detected");
    expect_string(selected_path, path.c_str(), "explicit config path is selected");

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    expect_int(kvs_agent_config_load_file(&config, selected_path), 0,
               "selected config file loads");
    expect_int(kvs_agent_config_parse_args(&config, argc, args), 0,
               "command-line overrides parse");
    expect_string(config.master_ip, "10.2.3.4", "CLI overrides master IP");
    expect_int(config.master_port, 19001, "CLI overrides master port");
    expect_string(config.listen_ip, "10.2.3.5", "CLI overrides listener IP");
    expect_int(config.listen_port, 20001, "CLI overrides listener port");
    expect_int(config.mode, 0, "CLI overrides capture mode");
    expect_int(config.master_pid, 321, "CLI overrides master PID");
    expect_string(config.master_binary, "./custom-kvstore", "CLI overrides master binary");
    expect_long(config.cache_max_records, 77L, "CLI overrides cache record limit");
    unlink(path.c_str());
}

static void test_rejects_orphan_config_option() {
    char program[] = "ebpf-agent";
    char config_option[] = "--config";
    char* args[] = {program, config_option};
    char selected_path[512];
    int has_explicit_path = 0;
    expect_true(kvs_agent_config_select_path(2, args, selected_path, sizeof(selected_path),
                                             &has_explicit_path) != 0,
                "--config without a path is rejected");
}

static void test_validation_requires_endpoints_and_mode_inputs() {
    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    expect_true(kvs_agent_config_validate(&config) != 0, "missing endpoints are rejected");

    strncpy(config.master_ip, "192.168.234.135", sizeof(config.master_ip) - 1);
    strncpy(config.listen_ip, "127.0.0.1", sizeof(config.listen_ip) - 1);
    config.mode = 1;
    strncpy(config.ring_path, "../data/kvstore.ebpf.ring", sizeof(config.ring_path) - 1);
    expect_int(kvs_agent_config_validate(&config), 0, "complete sim config is valid");

    config.mode = 0;
    expect_true(kvs_agent_config_validate(&config) != 0,
                "uprobe config without PID and binary is rejected");
}

static void test_replica_config_uses_agent_port() {
    const std::string path = make_temp_file(
        "role replica\n"
        "master_ip 192.168.234.135\n"
        "master_port 9999\n"
        "agent_port 12001\n"
        "rdma_advertise_host 192.168.234.200\n");
    expect_true(!path.empty(), "replica config fixture is created");
    if (path.empty()) {
        return;
    }

    char program[] = "kvstore";
    char config_option[] = "--config";
    char config_path[512];
    char agent_port_option[] = "--agent-port";
    char agent_port_value[] = "12002";
    char rdma_host_option[] = "--rdma-advertise-host";
    char rdma_host_value[] = "10.9.8.7";
    strncpy(config_path, path.c_str(), sizeof(config_path) - 1);
    config_path[sizeof(config_path) - 1] = '\0';
    char* args[] = {program,           config_option,   config_path,       agent_port_option,
                    agent_port_value,  rdma_host_option, rdma_host_value};
    int argc = static_cast<int>(sizeof(args) / sizeof(args[0]));
    char** argv = args;

    kvs_config_set_defaults();
    expect_int(kvs_config_agent_port(), 11001, "agent port defaults to 11001");
    expect_string(kvs_config_rdma_advertise_host(), "", "RDMA advertise host defaults to auto");
    expect_int(kvs_config_parse_switches(&argc, &argv), 0, "replica config parses");
    expect_int(kvs_config_agent_port(), 12002, "CLI overrides configured agent port");
    expect_string(kvs_config_rdma_advertise_host(), "10.9.8.7",
                  "CLI overrides configured RDMA advertise host");
    unlink(path.c_str());
}

int main() {
    test_loads_all_agent_fields();
    test_accepts_equals_and_inline_comments();
    test_rejects_unknown_key_and_invalid_port();
    test_finds_parent_config_for_build_launch();
    test_cli_overrides_loaded_config();
    test_rejects_orphan_config_option();
    test_validation_requires_endpoints_and_mode_inputs();
    test_replica_config_uses_agent_port();

    if (g_failures != 0) {
        return 1;
    }
    std::printf("ebpf-agent config tests passed\n");
    return 0;
}
