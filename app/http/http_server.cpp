#include "http_server.hpp"

http_server::http_server(uint16_t port, int core_num)
{
    mainreactor = &MainReactor::instance(port, core_num, [this](TCPConnection* tcpconnection){
        attach(tcpconnection);
    });
}

void http_server::attach(TCPConnection* connection)
{
    auto handler = new http_handler();      // ~http_handler() 实现了delete this
    handler->set_push_request([this](const HttpRequest& data){
        return push_response();
    });
    handler->bind_connection(connection);
}

HttpResponse http_server::push_response()
{
    // 先判断url是否存在/合法，然后再生成对应的httpresponse，目前是测试阶段，仅返回hello

    HttpResponse it{200};
    it.body = "hello";

    return it;
}

void http_server::run() { mainreactor->run(); }