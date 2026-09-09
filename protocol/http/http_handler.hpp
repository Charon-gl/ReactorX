#pragma once
#include <iostream>
#include "net/TCPConnection.hpp"
#include "llhttp.h"


class http_handler
{
private:
    llhttp_t* parser;
    TCPConnection *connection;
    
    // TCPConnection可能会向业务层发送关闭信号，所以还需要预留一个接口
    
    // 业务层接口
    std::function<void(TCPConnection *)> http_request;

public:
    http_handler(TCPConnection *);

    void set_get_request(std::function<void(TCPConnection *)> _cb);
};
