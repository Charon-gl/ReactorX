#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <errno.h>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include "core/Channel.hpp"
#include "core/IO_Object.hpp"
#include "reactor/SubReactor.hpp"
#include "utils/Err_Manager.hpp"

#define MAX_BUF_SIZE 1024

enum class Conn_state       // 新增连接状态机，定义以下连接状态
{
    UNCONNECT,          // 对象已创建但是未建立连接
    CONNECTED,          // 连接已建立    
    PROCESSING,         // 正在进行业务处理    
    CLOSING,            // 连接关闭中
};

class TCPConnection : public IO_Object
{
private:
    using Call_Process = std::function<void(TCPConnection *)>;
    using Call_Connection_Close = std::function<void(TCPConnection *, int)>;

    std::string recv_buf;
    std::string send_buf;
    size_t send_begin_pos;
    std::atomic<Conn_state> state;

    bool transition(Conn_state new_state);      //状态转换接口

// 事件处理函数
    void on_read() override;
    void on_send() override;
    bool on_error(int) override;
    void on_close(int err_no = 0) override;     // 框架层触发关闭
    void process_close(int err_no = 0);     // 业务层主动发起关闭

// 业务层接口
    Call_Process request_callback;    // 通知业务层取recvbuf数据进行处理
    Call_Connection_Close close_callback;   // 通知业务层连接已关闭

public:
    TCPConnection(SubReactor* reactor);

    void activate(int fd);      // 激活函数，绑定channel，初始化状态等
    void deactivate();      //注销函数，注销channel

    Conn_state get_state() const;   // 返回当前状态

    // recvbuf
    const char *peek() const;       // 返回recvbuf头部指针，要注意失效问题
    size_t readable_bytes() const;    // 返回recvbuf可读字节数
    void consume(size_t len);       // 消费recvbuf头部len个字节
    void clear_recvbuf();       // 清空recvbuf

    // sendbuf
    void append_buf(const char *data, size_t len);      // 将长度为len的数据追加到sendbuf
    size_t sendable_bytes() const;     // 返回sendbuf可发送字节数

    // process
    void set_request_callback(Call_Process _cb);
    void set_close_callback(Call_Connection_Close _cb);
    void enter_processing();        // 修改当前状态为PROCESSING，业务层使用，当前状态不是CONNECTED则切换无效
    void exit_processing();         // 修改当前状态为CONNECTED，业务层使用，当前状态不是PROCESSING则切换无效

    TCPConnection(TCPConnection &&) = delete;
    TCPConnection(const TCPConnection &) = delete;
    TCPConnection &operator=(TCPConnection &&) = delete;
    TCPConnection &operator=(const TCPConnection &) = delete;
    
    ~TCPConnection();
};