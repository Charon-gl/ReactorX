#pragma once
#include <unordered_map>
#include "reactor/MainReactor.hpp"
#include "http/http_handler.hpp"

class http_server
{
private:
    MainReactor* mainreactor;

    void attach(TCPConnection* connection);     // 工厂函数，创建httphandler实例

public:
    http_server(uint16_t port, int core_num);

    void run();

    HttpResponse push_response();
};