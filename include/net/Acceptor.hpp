#pragma once

#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include <functional>
#include <cstring>
#include <memory>
#include "core/Channel.hpp"
#include "core/IO_Object.hpp"
#include "utils/Err_Manager.hpp"

#define MAX_LISTEN_NUM 1024


class MainReactor;
class Acceptor : public IO_Object
{
private:
    uint16_t port;
    sockaddr_in addr;

    int init_listen_fd();
    
public:
    explicit Acceptor(MainReactor* mainreactor);

    void on_read() override;
    bool on_error(int err_no) override;
    void on_close(int err_no = 0) override;

    std::unique_ptr<Channel> active(uint16_t port);      // 激活acceptor
    void deactive();       // 重置acceptor

    Acceptor(Acceptor &&) = delete;
    Acceptor &operator=(Acceptor &&) = delete;
    Acceptor(const Acceptor &) = delete;
    Acceptor &operator=(const Acceptor &) = delete;

    ~Acceptor();
};