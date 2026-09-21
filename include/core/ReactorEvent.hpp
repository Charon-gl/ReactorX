#pragma once

#include <iostream>
#include <thread>
#include <variant>
#include <type_traits>
#include <string>
#include "net/ConnectionToken.hpp"
#include "utils/Err_Manager.hpp"



enum class CloseReason
{
    // 框架层
    NORMAL_ACTIVE,
    PEER_DISCONNECT,
    IO_ERROR,
    TIMEOUT,
    EPOLL_FATAL
};


// BusinessEvent
struct Request         // 连接的读缓冲区数据，传给上层业务
{
    ConnectionToken token;
    std::string data;
};

struct Response_Command         // 上层业务生成的响应，传给反应堆发送
{
    ConnectionToken token;
    std::string data;
};

struct Close_Command        // 上层业务主动关闭连接
{
    ConnectionToken token;
};

struct Reactor_Command
{
    std::variant<Response_Command, Close_Command> payload;
};


// ReactorEvent
struct NewConnectionEvent
{
    int connection_fd;
};

struct ConnectionCloseEvent     // 关闭业务连接 (cfd)，所有来源的关闭都要从这里进入
{
    //int fd;
    ConnectionToken token;
    CloseReason reason;
    int err_no;
};

struct ReactorFatalEvent        // Reactor 发起关闭事件
{
    uint32_t rector_id;
    int err_no;
};

struct LogEvent
{
    int level;
    std::string content;
};

using Event = std::variant<
    NewConnectionEvent,
    ConnectionCloseEvent,
    ReactorFatalEvent
    // LogEvent
>;


inline CloseReason get_reason(int err_no)
{
    switch (err_no)
    {
    case 0:
        return CloseReason::NORMAL_ACTIVE;
    case ETIMEDOUT:
        return CloseReason::TIMEOUT;
    case EPIPE:
    case ECONNRESET:
        return CloseReason::PEER_DISCONNECT;
    default:
        return CloseReason::IO_ERROR;
    }
}

