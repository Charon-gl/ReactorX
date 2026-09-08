#include "core/ReactorBase.hpp"

ReactorBase::ReactorBase()
{
    channels.reserve(MAX_PER_CONNECTION);
}

void ReactorBase::unregister_channel(int fd)
{
    while (true)
    {
        raw_epoll_ctl(EPOLL_CTL_DEL, nullptr, fd);
        break;
    }
    channels.erase(fd);
}

void ReactorBase::update_channel(Channel *channel)
{
    int fd = channel->get_fd();
    epoll_event ev;
    ev.data.fd = fd;
    ev.events = channel->get_events();
    while (true)
    {
        raw_epoll_ctl(EPOLL_CTL_MOD, &ev, fd);
        break;
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

void ReactorBase::connection_close_event(int fd, CloseReason reason, int err_no)
{
    unregister_channel(fd);
    // reason 和 err_no 用于记录日志
}

void ReactorBase::reactor_close_event(std::thread::id thread_id, int err_no) { stop_epoll(err_no); }

void ReactorBase::handle_event(const ReactorEvent &event)
{
    std::visit(overloaded{[this](const NewConnectionEvent &e)
                          { new_connection_event(e.connection_fd); },
                          [this](const ConnectionCloseEvent &e)
                          { connection_close_event(e.fd, e.reason, e.err_no); },
                          [this](const ReactorFatalEvent &e)
                          { reactor_close_event(e._id, e.err_no); }},
               event);
}

void ReactorBase::handle_error(const ErrorEvent &event)
{
    std::visit(overloaded{[](const NormalEvent &e) {},
                          [](const RetryableEvent &e) {},
                          [this](const ConnectionCloseEvent &e)
                          { connection_close_event(e.fd, e.reason, e.err_no); },
                          [this](const ReactorFatalEvent &e)
                          { reactor_fatal_event(e.err_no); }}, // 触发自己的reactor_fatal_event
               event);
}
