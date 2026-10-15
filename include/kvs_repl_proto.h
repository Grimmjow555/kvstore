#pragma once

// ==================== 复制通道握手 token ====================
//
// 握手负载本身也是 RESP 数组（外层仍是「4 字节长度头 + 负载」），由复制双方共用：
//
//   KVS_REPL_HANDSHAKE_EBPF     ebpf-agent -> Master   采集进程注册
//   KVS_REPL_HANDSHAKE_REPL_INC ebpf-agent -> Replica  增量通道建立
//
// Master 侧的识别在 replication/kvs_sync.cpp（kvs_sync_accept_*_handshake，
// 由 kvs_replication_accept_handshake 统一转发），发送侧在 tools/ebpf_agent.cpp。
// 两侧必须字面一致，因此放在这里而不是各自定义一份。

#define KVS_REPL_HANDSHAKE_EBPF "*1\r\n$4\r\nEBPF\r\n"
#define KVS_REPL_HANDSHAKE_REPL_INC "*1\r\n$8\r\nREPL_INC\r\n"
