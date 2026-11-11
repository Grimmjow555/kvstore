#pragma once

#include "capture/capture_types.h"

namespace kvs {
namespace capture {

// 采集生命周期门面。保持现有 sim/uprobe 后端和单一消费线程模型。
class CaptureRuntime {
    public:
    CaptureRuntime();
    ~CaptureRuntime();

    CaptureRuntime(const CaptureRuntime&) = delete;
    CaptureRuntime& operator=(const CaptureRuntime&) = delete;

    int start(const CaptureConfig& config, const RecordCallback& on_record,
              const ResyncCallback& on_resync);
    int reopen(const char* ring_path);
    void stop();

    private:
    static void record_trampoline(int argc, char* argv[]);
    static void resync_trampoline(const char* reason);

    RecordCallback on_record_;
    ResyncCallback on_resync_;
};

} // namespace capture
} // namespace kvs
