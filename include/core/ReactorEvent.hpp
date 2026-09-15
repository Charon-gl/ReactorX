#pragma once

#include <iostream>
#include <thread>
#include <variant>
#include <type_traits>
#include "utils/Err_Manager.hpp"

struct NewConnectionEvent
{
    int connection_fd;
};

enum class CloseReason
{
    NORMAL_ACTIVE,
    PEER_DISCONNECT,
    IO_ERROR,
    TIMEOUT,
    EPOLL_FATAL
};

struct ConnectionCloseEvent     // 关闭业务连接 (cfd)
{
    int fd;
    CloseReason reason;
    int err_no;
};

struct ReactorFatalEvent        // Reactor 发起关闭事件
{
    std::thread::id _id;
    int err_no;
};

struct LogEvent
{
    int level;
    std::string content;
};

using ReactorEvent = std::variant<
    NewConnectionEvent,
    ConnectionCloseEvent,
    ReactorFatalEvent
    // LogEvent
>;


//ErrorEvent
struct RetryableEvent {};

struct NormalEvent {};


using ErrorEvent = std::variant<
    RetryableEvent,
    NormalEvent,
    ConnectionCloseEvent,
    ReactorFatalEvent
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


inline ErrorEvent error_event_package(int err_no, Err_Rank err_rank, int fd = -1)
{
    switch (err_rank)
    {
    case Err_Rank::IGNORE:
        return NormalEvent{};
    case Err_Rank::RETRY:
        return RetryableEvent{};
    case Err_Rank::CLOSE_CONNECTION:
        return ConnectionCloseEvent{fd, get_reason(err_no), err_no};
    case Err_Rank::FATAL:
        return ReactorFatalEvent{std::this_thread::get_id(), err_no};
    }
}
