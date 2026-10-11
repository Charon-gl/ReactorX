#include "core/ReactorBase.hpp"

ReactorBase::ReactorBase()
{
    channels.reserve(MAX_PER_CONNECTION);
}

void ReactorBase::run() { loop(); }

void ReactorBase::unregister_channel(int fd)
{
    auto it = channels.find(fd);
    if(it != channels.end())
    {
        raw_epoll_ctl(EPOLL_CTL_DEL, nullptr, fd);
        it->second->deactivate();
    }
}

void ReactorBase::core_error(int err_no)        // 从EventLoop传来的错误
{
    auto err_rank = Err_Manager::err_judge(err_no);
    if (err_rank == Err_Rank::CLOSE_CONNECTION || err_rank == Err_Rank::FATAL)
    {
        stop_epoll(err_no);
        reactor_fatal_event();
    }    
}

void ReactorBase::io_event(int fd, uint32_t events)
{
    auto it = channels.find(fd);
    if (it != channels.end())
        it->second->handle_events(events);
}

void ReactorBase::reactor_close_event(uint32_t reactor_id, int err_no) { stop_epoll(err_no); }

void ReactorBase::handle_event(const Event &event)
{
    std::visit(overloaded{[this](const NewConnectionEvent &e)
                          { new_connection_event(e.connection_fd); },
                          [this](const ConnectionCloseEvent &e)
                          { connection_close_event(e.token, e.reason, e.err_no); },
                          [this](const ReactorFatalEvent &e)
                          { reactor_close_event(e.rector_id, e.err_no); }},
               event);
}