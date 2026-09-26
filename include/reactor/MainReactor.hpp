#pragma once

#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include "core/Channel.hpp"
#include "net/Acceptor.hpp"
#include "core/ReactorBase.hpp"
#include "net/FrameWorkDispatcher.hpp"
#include "SubReactor.hpp"

#define MAX_LISTENER_NUM 10


class SubReactor;

class MainReactor : public ReactorBase
{
private:
    using sub_reactor_set = std::unordered_map<uint32_t, std::unique_ptr<SubReactor>>;
    sub_reactor_set::iterator round_ptr;

    // 管理的对象
    std::unordered_map<uint32_t, std::unique_ptr<std::thread>> sub_reactor_threads;
    sub_reactor_set sub_reactors;
    std::unordered_map<int, std::unique_ptr<Acceptor>> active_listeners;

    
    void add_sub_reactor(FrameWorkDispatcher& dispatcher);
    void reactor_fatal_event(int err_no) override; // 删除单个sub_reactor
    void reactor_close_event(uint32_t reactor_id, int err_no) override;
    bool add_listener(uint16_t port);
    void update_round_ptr();

public:        
    MainReactor(uint16_t port, int core_num, FrameWorkDispatcher& dispatcher);

    void remove_listener(int fd, CloseReason reason, int err_no);
    // 轮询分发
    void round_dispatch(int fd);
    
    ~MainReactor();

    MainReactor(MainReactor &&) = delete;
    MainReactor &operator=(MainReactor &&) = delete;
    MainReactor(const MainReactor &) = delete;
    MainReactor &operator=(const MainReactor &) = delete;
};
/* 关闭逻辑：
1. subreactor异常而关闭：
    首先向mainreactor发送信号，（随后停止接收新连接），
    设置标志位，下一轮不再进行epollwait，
    逐步走channel断开连接的回调，或者直接清空channels
    析构subreactor

2. mainreactor主动关闭：
    收到信号，停止接收新连接
    逐个关闭连接，等待业务收尾
    执行完剩余任务
    释放资源
    向mainreactor确认


channel断开连接：
    绑定subreactor的dispatch回调，传入fd，reason，err_no
    即异步断开连接，避免在连接处理事件时释放自己而导致悬垂引用
*/