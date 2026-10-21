#include "http_handler.hpp"
#include <charconv>

// 请求体大小上限，超过则判定为非法请求
static constexpr size_t MAX_BODY_SIZE = 1 << 20;   // 1MB

// 将整数按十进制追加到 out，避免 std::to_string 产生的临时分配
template<typename T>
static void append_int(std::string& out, T value)
{
    char buf[32];
    auto res = std::to_chars(buf, buf + sizeof(buf), value);
    out.append(buf, res.ptr - buf);
}

void HttpRequest::reset()
{
    method.clear();
    url.clear();
    version.clear();
    headers.clear();
    body.clear();
    tmp.clear();
    field_tmp.clear();
    value_tmp.clear();
    keepalive = true;
}

const llhttp_settings_t& http_handler::setting()
{
    static llhttp_settings_t s = []{
        llhttp_settings_t tmp;
        llhttp_settings_init(&tmp);
        tmp.on_message_begin = &http_handler::on_message_begin;
        tmp.on_url = &http_handler::on_url;
        tmp.on_header_field = &http_handler::on_header_field;
        tmp.on_header_value = &http_handler::on_header_value;
        tmp.on_header_value_complete = &http_handler::on_header_value_complete;
        tmp.on_headers_complete = &http_handler::on_headers_complete;
        tmp.on_body = &http_handler::on_body;
        tmp.on_message_complete = &http_handler::on_message_complete;

        return tmp;
    }();

    return s;
}

http_handler::http_handler(Dispatcher* dispatcher)
    : parser(std::make_unique<llhttp_t>()), token_{}, dispatcher_(dispatcher)
{
    llhttp_init(parser.get(), HTTP_REQUEST, &setting());
    parser->data = this;
}

void http_handler::on_request(ByteStream stream)
{
    if(stream.token != token_)
        return;

    size_t len = stream.data.size();
    if(len == 0)
        return;

    auto res = llhttp_execute(parser.get(), stream.data.data(), len);

    if(res != HPE_OK)
    {
        // 请求非法，直接发送400并关闭连接
        response(HttpResponse{400}, true);
    }
}

void http_handler::activate(ConnectionToken token)
{
    token_ = token;
}

void http_handler::deactivate()
{
    token_ = {};
    request.reset();
    llhttp_reset(parser.get());
}

int http_handler::on_message_begin(llhttp_t* parser)
{// 主要用于新请求到来时（长连接）重置上下文
    auto it = static_cast<http_handler*>(parser->data);
    it->request.reset();

    return 0;
}

int http_handler::on_url(llhttp_t* parser, const char* at, size_t len)
{
    auto it = static_cast<http_handler*>(parser->data);
    it->request.url.append(at, len);

    return 0;
}

int http_handler::on_header_field(llhttp_t* parser, const char* at, size_t len)
{
    auto it = static_cast<http_handler*>(parser->data);
    it->request.field_tmp.append(at, len);

    return 0;
}

int http_handler::on_header_value(llhttp_t* parser, const char* at, size_t len)
{
    auto it = static_cast<http_handler*>(parser->data);
    it->request.value_tmp.append(at, len);

    return 0;
}

int http_handler::on_header_value_complete(llhttp_t* parser)
{// 一个头部的字段名和字段值都已收齐，提交到 headers 中
    auto it = static_cast<http_handler*>(parser->data);

    if(!it->request.field_tmp.empty())
        it->request.headers.emplace(std::move(it->request.field_tmp), std::move(it->request.value_tmp));
    else
    {   // 异常情况：字段名为空，直接丢弃本次字段值，保证缓冲区被清空
        it->request.field_tmp.clear();
        it->request.value_tmp.clear();
    }

    return 0;
}

int http_handler::on_headers_complete(llhttp_t* parser)
{/* 这里头部解析已经完成，但有些请求带有请求体，需要在此决定如何处理 body */
    auto it = static_cast<http_handler*>(parser->data);

    // 仅对带 Content-Length 的请求体做大小校验，chunked 暂不限制
    if((parser->flags & F_CHUNKED) == 0 && parser->content_length > MAX_BODY_SIZE)
    {
        llhttp_set_error_reason(parser, "Request body too large");
        return -1;      // 触发 HPE_USER，停止解析
    }

    return 0;   // 有请求体则继续 on_body，否则直接进入 on_message_complete
}

int http_handler::on_body(llhttp_t* parser, const char* at, size_t len)
{
    auto it = static_cast<http_handler*>(parser->data);
    it->request.body.append(at, len);

    return 0;
}

int http_handler::on_message_complete(llhttp_t* parser)
{// 请求解析完成，交给业务层处理并回写响应
    auto it = static_cast<http_handler*>(parser->data);

    // 填充请求行信息
    it->request.method = llhttp_method_name(static_cast<llhttp_method_t>(llhttp_get_method(parser)));
    it->request.version = "HTTP/";
    it->request.version.push_back(static_cast<char>('0' + llhttp_get_http_major(parser)));
    it->request.version.push_back('.');
    it->request.version.push_back(static_cast<char>('0' + llhttp_get_http_minor(parser)));
    it->request.keepalive = llhttp_should_keep_alive(parser);

    if(it->push_request_async)
    {
        auto* dispatcher = it->dispatcher_;
        const auto token = it->token_;
        const auto major = llhttp_get_http_major(parser);
        const auto minor = llhttp_get_http_minor(parser);
        const bool is_close = !it->request.keepalive;
        it->push_request_async(it->request,
            [dispatcher, token, major, minor, is_close](HttpResponse response_data){
                submit_response(dispatcher, token, response_data, is_close, major, minor);
            });
    }
    else if(it->push_request)
    {
        const auto& response_data = it->push_request(it->request);
        it->response(response_data, !it->request.keepalive);
    }

    return 0;
}

void http_handler::response(const HttpResponse& data, bool is_close)
{
    submit_response(dispatcher_, token_, data, is_close,
                    llhttp_get_http_major(parser.get()), llhttp_get_http_minor(parser.get()));
}

void http_handler::submit_response(Dispatcher* dispatcher, ConnectionToken token,
                                   const HttpResponse& data, bool is_close,
                                   uint8_t major, uint8_t minor)
{
    // 预分配，避免拼接过程中多次扩容重分配（响应头约 128 字节 + body）
    std::string response_buf;
    response_buf.reserve(data.body.size() + 128);

    // 版本号（解析失败时可能为 0.0，默认按 1.1 处理）
    if(major == 0 && minor == 0)
    {
        major = 1;
        minor = 1;
    }

    // 状态行：HTTP/1.1 200 OK\r\n
    response_buf.append("HTTP/");
    response_buf.push_back(static_cast<char>('0' + major));
    response_buf.push_back('.');
    response_buf.push_back(static_cast<char>('0' + minor));
    response_buf.push_back(' ');
    append_int(response_buf, data.status);
    response_buf.push_back(' ');
    auto reason_it = status_reason.find(data.status);
    if(reason_it != status_reason.end())
        response_buf.append(reason_it->second);
    else
        response_buf.append("Unknown");
    response_buf.append("\r\n");

    // 响应头
    response_buf.append("Content-Length: ");
    append_int(response_buf, data.body.size());
    response_buf.append("\r\n");
    response_buf.append(is_close ? "Connection: close\r\n" : "Connection: keep-alive\r\n");

    if(!data.headers.empty())
    {
        for(const auto& [k, v] : data.headers)
        {
            response_buf.append(k);
            response_buf.append(": ");
            response_buf.append(v);
            response_buf.append("\r\n");
        }
    }
    response_buf.append("\r\n");

    // 响应体
    response_buf.append(data.body);

    // 顺序很重要：必须先投递关闭，再投递发送。
    // 框架中 to_close 只有在 request_count == 0 时才会真正关闭连接，
    // 而 to_send 会把 request_count 减到 0，因此先 close 后 send，
    // 才能保证响应全部发送完成后连接才被关闭，避免响应丢失。
    if(is_close)
        dispatcher->submit_operation(Operation{Close_Operation{token}});
    dispatcher->submit_operation(Operation{Send_Operation{token, std::move(response_buf)}});

    return;
}

void http_handler::set_push_request(std::function<HttpResponse(const HttpRequest&)> _cb)
{
    push_request = std::move(_cb);
    push_request_async = {};
}

void http_handler::set_push_request_async(std::function<void(const HttpRequest&, response_callback)> cb)
{
    push_request_async = std::move(cb);
    push_request = {};
}

http_handler::~http_handler()
{

}
