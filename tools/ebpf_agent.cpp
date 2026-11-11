#include "agent/agent_service.h"
#include "kvs_agent_config.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static volatile sig_atomic_t g_running = 1;

static void on_signal(int) { g_running = 0; }

static void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s [--config <path>] [--master <ip:port>] [--listen <ip:port>]\n"
            "          [--mode <uprobe|sim>]\n"
            "          [--ring <path>] [--pid <master_pid>] [--binary <master_exe>]\n"
            "          [--cache-max-bytes N] [--cache-max-records N]\n"
            "          [--max-replicas N] [--memory-budget-bytes N]\n",
            prog);
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);
    char config_path[512];
    int has_explicit_path = 0;
    if (kvs_agent_config_select_path(argc, argv, config_path, sizeof(config_path),
                                     &has_explicit_path) != 0) {
        usage(argv[0]);
        return 2;
    }

    int config_rc = has_explicit_path ? kvs_agent_config_load_file(&config, config_path)
                                      : kvs_agent_config_load_default(&config);
    if (config_rc < 0 ||
        kvs_agent_config_parse_args(&config, argc, argv) != 0 ||
        kvs_agent_config_validate(&config) != 0) {
        usage(argv[0]);
        return 2;
    }

    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = on_signal;
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    signal(SIGPIPE, SIG_IGN);

    kvs::agent::AgentService service(config);
    return service.run(&g_running);
}
