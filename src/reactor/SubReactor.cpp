#include "reactor/SubReactor.hpp"

/*-----------------SubReactor-----------------*/

SubReactor::SubReactor(std::function<void(TCPConnection*)> business_handler)
    : business_handler(std::move(business_handler))
{
    connections.reserve(INIT_CONNECTION_NUM);
}

void SubReactor::new_connection_event(int fd)
{
    // 创建TCPConnection，绑定回调，加入connectionmanagers
    auto it = std::make_unique<TCPConnection>(this);
    it->activate(fd);

    business_handler(it.get());     // 将tcpconnection的指针提供给业务层绑定
    
    connections.emplace(fd, std::move(it));
}

void SubReactor::connection_close_event(int fd, CloseReason reason, int err_no)
{
    auto it = connections.find(fd);
    if (it != connections.end())
    {
        if(!it->second->is_vaild())
        {
            connections.erase(it);
            unregister_channel(fd);
        }
    }
}

void SubReactor::reactor_fatal_event(int err_no)
{
    stop_epoll(err_no);
}

void SubReactor::set_call_main_reactor(std::function<void(const ReactorFatalEvent&)> _cb) { call_main_reactor = std::move(_cb); }


SubReactor::~SubReactor()
{
    call_main_reactor(ReactorFatalEvent{std::this_thread::get_id(), fatal_errno});
}