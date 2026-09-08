#pragma once

#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include "core/Channel.hpp"
#include "core/ReactorBase.hpp"
#include "net/TCPConnection.hpp"

#define INIT_CONNECTION_NUM 1024



class SubReactor : public ReactorBase
{
private:
    std::unordered_map<int, std::unique_ptr<TCPConnection>> connections;

    std::function<void(std::thread::id, int)> call_main_reactor; // 从reactor给主reactor通信的回调接口
    void new_connection_event(int fd) override;
    void reactor_fatal_event(int err_no) override;

protected:
public:
    SubReactor();

    void run();

    void connection_close_event(int fd, CloseReason reason, int err_no) override;

    template <typename F>
    void set_call_main_reactor(F &&_cb)
    {
        call_main_reactor = std::forward<F>(_cb);
    }

    ~SubReactor();

    SubReactor(SubReactor &&) = delete;
    SubReactor &operator=(SubReactor &&) = delete;
    SubReactor(const SubReactor &) = delete;
    SubReactor &operator=(const SubReactor &) = delete;
};
