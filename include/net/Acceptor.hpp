#pragma once

#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include <functional>
#include <cstring>
#include <memory>
#include "core/Channel.hpp"
#include "core/IO_Object.hpp"
#include "reactor/MainReactor.hpp"
#include "utils/Err_Manager.hpp"


class Acceptor : public IO_Object
{
private:
    sockaddr_in addr;

    int init_listen_fd();
    void accept_fd();
    
public:
    explicit Acceptor(MainReactor* mainreactor);

    void on_read() override;
    bool on_error(int err_no) override;
    void on_close(int err_no = 0) override;

    bool active(const sockaddr_in& addr);      // 激活acceptor
    void deactive();       // 重置acceptor

    
    Acceptor(Acceptor &&) = delete;
    Acceptor &operator=(Acceptor &&) = delete;
    Acceptor(const Acceptor &) = delete;
    Acceptor &operator=(const Acceptor &) = delete;

    ~Acceptor();
};