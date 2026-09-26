#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <errno.h>
#include <functional>
#include <unordered_map>
#include "core/IO_Object.hpp"
#include "utils/Err_Manager.hpp"

#define MAX_BUF_SIZE 1024


enum class Conn_state       // 新增连接状态机，定义以下连接状态
{
    UNCONNECT,          // 对象已创建但是未建立连接
    CONNECTED,          // 连接已建立    
    //PROCESSING,         // 正在进行业务处理    
    CLOSING            // 连接不再接收请求，发送完响应后就关闭
};
  

class SubReactor;

struct ConnectionInit
{
    SubReactor* reactor;
    std::function<void(Request)> _cb;
};

class TCPConnection : public IO_Object
{
private:
    ConnectionToken token;
    std::string recv_buf;
    std::string send_buf;
    size_t send_begin_pos;      // 尚未发送的数据起始位置
    Conn_state state;
    counter request_count;       // 记录提交的请求数
    bool close_event_sent;


    bool transition(Conn_state new_state);      //状态转换函数

// 事件处理函数
    void on_read() override;
    void on_send() override;
    bool on_error(int) override;
    void on_close(int err_no = 0) override;     // 框架层触发关闭
    
    std::function<void(Request)> submit_request;        // 提交请求

public:
    TCPConnection(SubReactor*, std::function<void(Request)> request_callback);

    void activate(Channel* channel, ConnectionToken token_);      // 激活函数，绑定channel，初始化状态等
    void deactivate();      //注销函数，注销channel

    Conn_state get_state() const { return state; }   // 返回当前状态
    ConnectionToken get_token() const { return token; }
    
    // process
    void set_request_submit(std::function<void(Request)> _cb) { submit_request = std::move(_cb); }

    // send
    void to_send(const char* data, size_t len);
    void to_close();

    TCPConnection(TCPConnection &&) = delete;
    TCPConnection(const TCPConnection &) = delete;
    TCPConnection &operator=(TCPConnection &&) = delete;
    TCPConnection &operator=(const TCPConnection &) = delete;
    
    ~TCPConnection() = default;
};