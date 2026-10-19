#pragma once

#include <stddef.h>

// ==================== 复制相关进程共用的极简 TCP 工具 ====================
//
// 实现位于 replication/kvs_netutil.cpp，同时编入 kvstore 与 ebpf-agent。
//
// 约定：
//   * timeout_ms <= 0 表示无限等待；
//   * socket 使用「非阻塞 connect + 超时」建立，之后保持阻塞模式，并把超时写入
//     SO_SNDTIMEO / SO_RCVTIMEO；
//   * 失败一律返回 -1；kvs_net_recv_frame 成功返回负载长度。

#ifdef __cplusplus
extern "C" {
#endif

// 连接 ip:port；超时或失败返回 -1。
int kvs_net_connect(const char* ip, int port, int timeout_ms);

// 发一帧：外层协议是「4 字节网络序长度 + 负载」。
int kvs_net_send_frame(int fd, const char* payload, size_t len, int timeout_ms);

// 收一帧，负载写入 out（以 '\0' 结尾）；成功返回负载长度（>=1），失败返回 -1。
// out_size 同时充当最大帧长限制。
int kvs_net_recv_frame(int fd, char* out, size_t out_size, int timeout_ms);

// 收一帧并判断是否被受理（负载以 '+' 开头，例如 "+OK\r\n"）。
// 返回 1 = 受理，0 = 对端拒绝或响应非法，-1 = 收包失败。
int kvs_net_recv_frame_ok(int fd, int timeout_ms);

#ifdef __cplusplus
}
#endif
