#include "capture/capture_runtime.h"

#include "kvs_ebpf_agent.h"

namespace kvs {
namespace capture {

namespace {

CaptureRuntime* g_runtime = nullptr;

} // namespace

CaptureRuntime::CaptureRuntime() {}

CaptureRuntime::~CaptureRuntime() { stop(); }

int CaptureRuntime::start(const CaptureConfig& config, const RecordCallback& on_record,
                          const ResyncCallback& on_resync) {
    on_record_ = on_record;
    on_resync_ = on_resync;
    g_runtime = this;
    return kvs_ebpf_agent_capture_start(config.mode, config.ring_path, config.master_pid,
                                        config.master_binary, record_trampoline,
                                        resync_trampoline);
}

int CaptureRuntime::reopen(const char* ring_path) {
    return kvs_ebpf_agent_capture_reopen(ring_path);
}

void CaptureRuntime::stop() {
    kvs_ebpf_agent_capture_stop();
    if (g_runtime == this) {
        g_runtime = nullptr;
    }
}

void CaptureRuntime::record_trampoline(int argc, char* argv[]) {
    if (g_runtime && g_runtime->on_record_) {
        g_runtime->on_record_(argc, argv);
    }
}

void CaptureRuntime::resync_trampoline(const char* reason) {
    if (g_runtime && g_runtime->on_resync_) {
        g_runtime->on_resync_(reason);
    }
}

} // namespace capture
} // namespace kvs
