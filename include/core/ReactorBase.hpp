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

    
    Channel* try_get_channel(int fd)
    {// fd直接作为key，若找不到则创建一个channel
        uint32_t events = EPOLLET | EPOLLIN | EPOLLRDHUP;
        
        auto it = channels.find(fd);
        if(it != channels.end())
        {
            it->second->activate(fd, events);
            epoll_event ev;
            ev.data.fd = fd;
            ev.events = events;          // 注册事件在activate()完成
            raw_epoll_ctl(EPOLL_CTL_ADD, &ev, fd);   // 加入到epoll实例

            return it->second.get();
        }
        
        auto channel = std::make_unique<Channel>();
        channel->set_update_events([this](Channel* channel) { 
            int fd = channel->get_fd();
            epoll_event ev;
            ev.data.fd = fd;
            ev.events = channel->get_events();
        
            raw_epoll_ctl(EPOLL_CTL_MOD, &ev, fd); 
        });

        channel->activate(fd, events);
        epoll_event ev;
        ev.data.fd = fd;
        ev.events = events;          // 注册事件在activate()完成
        raw_epoll_ctl(EPOLL_CTL_ADD, &ev, fd);        // 加入到epoll实例

        Channel* raw_channel = channel.get();
        channels.emplace(fd, std::move(channel));

        return raw_channel;
    }

    void unregister_channel(int fd);

protected:
    counter reactor_counter;        // 维护所管理对象的标识号，只增不减
    void io_event(int fd, uint32_t events) override;
    
private:
    void handle_event(const Event &);
    void core_error(int err_no) override;

    virtual void connection_close_event(const ConnectionToken& token, const CloseReason& reason, int err_no) {}
    virtual void new_connection_event(int fd) {}
    virtual void reactor_fatal_event() = 0;         // epollfd出错，需要整个reactor关闭
    virtual void reactor_close_event(uint32_t reactor_id, int err_no);
    // virtual void on_log_event(int level, std::string content) {}

    std::unordered_map<int, std::unique_ptr<Channel>> channels; // 存储业务channel
};