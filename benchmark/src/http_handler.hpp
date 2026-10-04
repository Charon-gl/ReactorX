#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <utility>
#include <string_view>
#include "core/ReactorEvent.hpp"
#include "net/TCPConnection.hpp"
#include "net/Dispatcher.hpp"
#include "llhttp.h"

struct HttpRequest
{
    // 请求行
    std::string method;
    std::string url;
    std::string version;
    // 请求头
    std::unordered_map<std::string, std::string> headers;
    // 请求体
    std::string body;

    bool keepalive{true};
    std::string tmp;            // 通用缓冲区
    std::string field_tmp;      // header字段缓冲区
    std::string value_tmp;      // header字段值缓冲区

    void reset();
};

struct HttpResponse
{
    int status;
    std::vector<std::pair<std::string, std::string>> headers;
    // 用 string_view 引用响应体，避免拷贝。
    // 注意：被引用的数据（如静态缓存）必须比本结构存活更久。
    std::string_view body;

    HttpResponse() : status(200) {}
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
    Dispatcher* dispatcher_;
    HttpRequest request;
    ConnectionToken token_;

    // 绑定到settings的回调
    static int on_message_begin(llhttp_t* parser);
    static int on_url(llhttp_t*, const char* at, size_t len);
    static int on_header_field(llhttp_t*, const char* at, size_t len);   // 收到头部字段名
    static int on_header_value(llhttp_t*, const char* at, size_t len);   // 收到头部字段值
    static int on_header_value_complete(llhttp_t*);                      // 头部字段值收齐，提交
    static int on_headers_complete(llhttp_t*);
    static int on_body(llhttp_t*, const char* at, size_t len);           // 请求体
    static int on_message_complete(llhttp_t*);


    // 业务层接口
    std::function<HttpResponse(const HttpRequest&)> push_request;

public:
    static const llhttp_settings_t& setting();

    http_handler(Dispatcher* dispatcher);

    // 绑定/解绑一个连接。解绑只重置 token 与解析状态，不销毁对象，
    // 以便连接关闭后由上层放回对象池复用（避免反复申请内存）。
    void activate(ConnectionToken token);
    void deactivate();

    // 解析一条到达的数据流
    void on_request(ByteStream stream);

    void set_push_request(std::function<HttpResponse(const HttpRequest&)> _cb);

    void response(const HttpResponse&, bool is_close = false);

    ~http_handler();
};
