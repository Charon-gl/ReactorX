#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "net/TCPConnection.hpp"
#include "llhttp.h"

struct HttpRequest
{
    // 请求头
    std::string method;
    std::string url;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    // 请求体
    std::string body;

    bool keepalive{false};
    std::string tmp;            // 通用缓冲区
    std::string field_tmp;      // header字段缓冲区
    std::string value_tmp;      // header字段值缓冲区

    void reset();
};

struct HttpResponse
{
    int status;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    HttpResponse() {}
    HttpResponse(int _status) : status(_status) {}
};

static std::unordered_map<int, std::string> status_reason = 
{
    {200, "OK"}, 
    {400, "Bad Request"},
    {403, "Forbidden"},
    {404, "Not Found"},
    {500, "Internal Server Error"},
    {503, "Service Unavailable"}
};

class http_handler
{
private:
    std::unique_ptr<llhttp_t> parser;
    TCPConnection* connection;
    HttpRequest request;
    
    // 异步情况下TCPConnection可能会向业务层发送关闭信号，所以还需要预留一个接口
    

    // 绑定到settings的回调
    static int on_message_begin(llhttp_t* parser);
    static int on_url(llhttp_t*, const char* at, size_t len);
    static int on_header_field(llhttp_t*, const char* at, size_t len);   // 收到头部字段名
    static int on_header_value(llhttp_t*, const char* at, size_t len);   // 收到头部字段值
    static int on_headers_complete(llhttp_t*);
    static int on_body(llhttp_t*, const char* at, size_t len);           // 请求体
    static int on_message_complete(llhttp_t*);


    // 业务层接口
    std::function<HttpResponse(const HttpRequest&)> push_request;

public:
    static const llhttp_settings_t& setting();

    http_handler();

    void bind_connection(TCPConnection *);

    void set_push_request(std::function<HttpResponse(const HttpRequest&)> _cb);

    void response(const HttpResponse&);

    ~http_handler();
};
