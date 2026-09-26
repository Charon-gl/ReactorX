#pragma once

#include <iostream>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <functional>

enum class Err_Rank
{
    IGNORE,    // 忽略
    RETRY,     // 重试
    CLOSE_CONNECTION,  // 断开连接
    FATAL // 释放全部连接
};

class Err_Manager
{
public:
    Err_Manager(const Err_Manager &) = delete;
    Err_Manager(Err_Manager &&) = delete;

    static Err_Rank err_judge(int err_no);

private:
    Err_Manager() = default;
};