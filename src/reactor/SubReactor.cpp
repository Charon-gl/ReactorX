#include "reactor/SubReactor.hpp"

/*-----------------SubReactor-----------------*/

SubReactor::SubReactor()
{
    connections.reserve(INIT_CONNECTION_NUM);
}

void SubReactor::run() { loop(); }

void SubReactor::new_connection_event(int fd)
{
    // 创建TCPConnection，绑定回调，加入connectionmanagers
    auto it = std::make_unique<TCPConnection>(this);
    it->activate(fd);
    connections.emplace(fd, std::move(it));
}

void SubReactor::connection_close_event(int fd, CloseReason reason, int err_no)
{
    auto it = connections.find(fd);
    if (it != connections.end())
    {
        connections.erase(it);
        unregister_channel(fd);
    }
}

void SubReactor::reactor_fatal_event(int err_no)
{
    stop_epoll(err_no);
}

SubReactor::~SubReactor()
{
    call_main_reactor(std::this_thread::get_id(), fatal_errno);
}