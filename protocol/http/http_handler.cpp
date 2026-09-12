#include "protocol/http/http_handler.hpp"


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
        tmp.on_headers_complete = &http_handler::on_headers_complete;
        tmp.on_body = &http_handler::on_body;
        tmp.on_message_complete = &http_handler::on_message_complete;

        return tmp;
    }();

    return s;
}

http_handler::http_handler(TCPConnection* tcpconnection)
    : parser(std::make_unique<llhttp_t>()), connection(tcpconnection)
{
    llhttp_init(parser.get(), HTTP_REQUEST, &setting());
    parser->data = this;

    // tcpconnection的回调
    connection->set_request_callback([this]{
        const char* data = connection->peek();
        size_t len = connection->readable_bytes();

        if(len == 0)
            return;

        auto res = llhttp_execute(parser.get(), data, len);
        if(res != HPE_OK)  // 请求非法，直接发送400并主动断开连接
        {
            response(HttpResponse{400});
            connection->process_close();
        }
    });
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

    auto field = it->request.field_tmp;
    if(!field.empty())
    {
        it->request.headers.emplace(field, std::string(at, len));
        field.clear();
    }

    return 0;
}

int http_handler::on_headers_complete(llhttp_t* parser)
/* 在这里头部的解析就已经完成了，但是有些请求是带有请求体的，
    因此在此处要判断需不需要处理body，以及该如何处理body*/
{
    auto it = static_cast<http_handler*>(parser->data);
    auto method = llhttp_get_method(parser);
    //if(method == HTTP_GET || method == HTTP_HEAD)
        return 1;       // 没有请求体


    // 以下内容在单线程测试通过后再完成
    // // 有请求体但是长度超过上限，直接返回-1停止解析
    // return -1;

    // return 0;   // 有请求体，正常返回继续on_body逻辑
}

int http_handler::on_body(llhttp_t* parser, const char* at, size_t len)
{
    auto it = static_cast<http_handler*>(parser->data);
    it->request.body.append(at, len);

    return 0;
}

int http_handler::on_message_complete(llhttp_t* parser)
{// 请求解析完成，进入processing状态
    auto it = static_cast<http_handler*>(parser->data);
    it->connection->enter_processing();

    //将解析好的请求报文交给业务层
    if(it->push_request)
        it->push_request(it->request);

    return 0;
}

void http_handler::response(const HttpResponse& data)
{
    std::string response_buf;
    std::string reason = status_reason[data.status];        // 可选要不要做检查
    // 版本号
    response_buf += "HTTP/" 
                    + std::to_string(llhttp_get_http_major(parser.get())) 
                    + '.' 
                    + std::to_string(llhttp_get_http_minor(parser.get())); 
    // 状态码 + 原因
    response_buf += ' ' + std::to_string(data.status) + ' ' + reason + "\r\n";

    // 响应头
    response_buf += "Content-Length: " + std::to_string(data.body.size()) + "\r\n";
    
    if(!data.headers.empty())
    {
        for(const auto& [k, v] : data.headers)
        response_buf += k + ": " + v + "\r\n";
    }
    response_buf += "\r\n";

    // 响应体
    response_buf += data.body;

    connection->append_buf(response_buf.data(), response_buf.size());
    return;
}

void http_handler::set_push_request(std::function<void(const HttpRequest&)> _cb) { push_request = std::move(_cb); }