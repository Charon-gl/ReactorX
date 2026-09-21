#pragma once

#include <iostream>
#include <memory>
#include <unordered_map>
#include <variant>
#include <type_traits>
#include "Channel.hpp"
#include "EventLoop.hpp"
#include "ReactorEvent.hpp"
#include "utils/Err_Manager.hpp"
#include "utils/counter.hpp"


template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};

template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;


class ReactorBase : protected EventLoop          // 该类专门实现Reactor的任务分配逻辑，即拿到跨线程投递的任务后进入什么处理逻辑
{
public:
    ReactorBase();

    void run();

    template <typename Event>
    void dispatch_event(Event&& event)
    {
        run_in_loop([this, event = Event(std::forward<Event>(event))] { 
            handle_event(event); 
        });
    }

    template <typename T>
    void register_channel(T &&channel)
    {
        channel->set_update_events([this](Channel* channel) { 
            int fd = channel->get_fd();
            epoll_event ev;
            ev.data.fd = fd;
            ev.events = channel->get_events();
        
            raw_epoll_ctl(EPOLL_CTL_MOD, &ev, fd); 
        });

        int fd = channel->get_fd();
        epoll_event ev;
        ev.data.fd = fd;
        ev.events = channel->get_events();
        while (true)        // 加入到epoll实例
        {
            bool res = raw_epoll_ctl(EPOLL_CTL_ADD, &ev, fd);
            if (!res)
                return;
            break;
        }

        channel->enable_events(EPOLLIN | EPOLLRDHUP);   // epollrdhup的处理入口都是trigger_read()，因此就算是listenfd注册了也无伤大雅
        channels.emplace(fd, std::forward<T>(channel));
    }

    void unregister_channel(int fd);

protected:
    counter reactor_counter;        // 该计数器用于reactor编号，因此只增不减
    void io_event(int fd, uint32_t events) override;
    
private:
    void handle_event(const Event &);
    void core_error(int err_no) override;

    virtual void connection_close_event(const ConnectionToken& token, const CloseReason& reason, int err_no) {}
    virtual void new_connection_event(int fd) {}
    virtual void reactor_fatal_event(int err_no) = 0; // epollfd出错，需要整个reactor关闭
    virtual void reactor_close_event(uint32_t, int err_no);
    // virtual void on_log_event(int level, std::string content) {}

    std::unordered_map<int, std::unique_ptr<Channel>> channels; // 存储业务channel
};