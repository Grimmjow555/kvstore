#pragma once

#include <functional>

namespace kvs {
namespace capture {

typedef std::function<void(int, char*[])> RecordCallback;
typedef std::function<void(const char*)> ResyncCallback;

struct CaptureConfig {
    int mode;
    const char* ring_path;
    int master_pid;
    const char* master_binary;
};

} // namespace capture
} // namespace kvs
