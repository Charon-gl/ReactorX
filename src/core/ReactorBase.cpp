#include "core/ReactorBase.hpp"

ReactorBase::ReactorBase()
{
    channels.reserve(MAX_PER_CONNECTION);
}

void ReactorBase::run() { loop(); }

void ReactorBase::unregister_channel(int fd)
{
    if(fd != -1)
    {
        raw_epoll_ctl(EPOLL_CTL_DEL, nullptr, fd);
        channels.erase(fd);
    }
}

void ReactorBase::core_error(int err_no)
{
    auto err_rank = Err_Manager::err_judge(err_no);
    if (err_rank == Err_Rank::CLOSE_CONNECTION || err_rank == Err_Rank::FATAL)
        reactor_fatal_event(err_no);
}

void ReactorBase::io_event(int fd, uint32_t events)
{
    auto it = channels.find(fd);
    if (it != channels.end())
        it->second->handle_events(events);
}

void ReactorBase::reactor_close_event(uint32_t, int err_no) { stop_epoll(err_no); }

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