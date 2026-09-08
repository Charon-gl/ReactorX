#pragma once

#include <iostream>
#include <memory>
#include <unordered_map>
#include <variant>
#include <type_traits>
#include "Channel.hpp"
#include "core/EventLoop.hpp"
#include "core/ReactorEvent.hpp"
#include "utils/Err_Manager.hpp"


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

    template <typename Event>
    void dispatch_event(Event&& event)
    {
        run_in_loop([this, event = ReactorEvent(std::forward<Event>(event))] { 
            handle_event(event); 
        });
    }

    template <typename Event>
    void dispatch_error_event(Event&& event)
    {
        run_in_loop([this, event = ErrorEvent(std::forward<Event>(event))] { 
            handle_error(event); 
        });
    }

    template <typename T>
    void register_channel(T &&channel)
    {
        int fd = channel->get_fd();
        epoll_event ev;
        ev.data.fd = fd;
        ev.events = channel->get_events();

        channel->set_update_events([this, channel] { 
            update_channel(channel.get()); 
        });

        while (1)
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
    void update_channel(Channel *channel);

protected:
    void io_event(int fd, uint32_t events) override;
    
private:
    void handle_event(const ReactorEvent &);
    void handle_error(const ErrorEvent&);
    void core_error(int err_no) override;

    virtual void io_event(int fd, uint32_t events) = 0;
    virtual void connection_close_event(int fd, CloseReason reason, int err_no);
    virtual void new_connection_event(int fd) {}
    virtual void reactor_fatal_event(int err_no) = 0; // epollfd出错，需要整个reactor关闭
    virtual void reactor_close_event(std::thread::id thread_id, int err_no);
    // virtual void on_log_event(int level, std::string content) {}

    std::unordered_map<int, std::unique_ptr<Channel>> channels; // 存储业务channel
};