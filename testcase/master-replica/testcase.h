#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// 循环发送直到全部数据发出或失败
int send_all(int fd, const void* buffer, size_t length) {
    const char* ptr = (const char*)buffer;
    size_t sent = 0;
    while (sent < length) {
        ssize_t n = send(fd, ptr + sent, length - sent, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR)
                continue; // 被信号中断，重试
            return -1;    // 发送失败
        }
        sent += n;
    }
    return 0;
}

int send_msg(int connfd, const char* msg, int length) {
    if (msg == NULL || length < 0)
        return -1;

    // 将长度转换为网络字节序
    uint32_t net_len = htonl((uint32_t)length);
    if (send_all(connfd, &net_len, sizeof(net_len)) != 0) {
        return -1; // 头部发送失败
    }
    if (send_all(connfd, msg, length) != 0) {
        return -1; // 数据发送失败
    }
    return 0; // 成功
}

// 辅助函数：确保完整接收指定长度的数据
// 返回值：0 表示成功，-1 表示出错，0 也可能表示对端关闭（需通过返回值判断）
int recv_all(int fd, void* buffer, size_t length) {
    char* ptr = (char*)buffer;
    size_t received = 0;
    while (received < length) {
        ssize_t n = recv(fd, ptr + received, length - received, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue; // 被信号中断，重试
            return -1;    // 真正的错误
        } else if (n == 0) {
            // 对端关闭连接，且尚未读满数据
            return -1; // 可以根据需要返回特殊值
        }
        received += n;
    }
    return 0;
}

// 接收一条完整消息
// 参数：
//   connfd - 连接套接字
//   msg    - 接收缓冲区（由调用者提供）
//   length - 缓冲区大小（字节数）
// 返回值：
//   >0     - 成功，返回实际接收到的消息体长度
//   -1     - 出错或对端关闭
//   -2     - 缓冲区不足（消息体长度超过 length）
int recv_msg(int connfd, char* msg, int length) {
    if (msg == NULL || length < 0)
        return -1;

    // 1. 先接收 4 字节长度前缀
    uint32_t net_len;
    if (recv_all(connfd, &net_len, sizeof(net_len)) != 0) {
        return -1; // 接收头部失败或连接关闭
    }
    uint32_t data_len = ntohl(net_len); // 转换为本地字节序

    // 2. 检查缓冲区是否足够
    if (data_len > (uint32_t)length) {
        // 缓冲区太小，无法容纳完整消息
        // 实际项目中可以选择动态分配或丢弃剩余数据，这里简单返回 -2
        return -2;
    }

    // 3. 按长度读取消息体
    if (recv_all(connfd, msg, data_len) != 0) {
        return -1; // 接收数据失败或连接关闭
    }

    // 4. 可选：为字符串添加结束符（如果协议是文本）
    msg[data_len] = '\0'; // 注意：可能超出缓冲区1字节，需确保 length > data_len

    return (int)data_len;
}

// int send_msg(int connfd, const char* msg, int length) {

//     int res = send(connfd, msg, length, 0);
//     if (res < 0) {
//         perror("send");
//         exit(1);
//     }
//     return res;
// }

// int recv_msg(int connfd, char* msg, int length) {

//     int res = recv(connfd, msg, length, 0);
//     if (res < 0) {
//         perror("recv");
//         exit(1);
//     }
//     return res;
// }