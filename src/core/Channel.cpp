#include "core/Channel.hpp"

Channel::Channel(int _fd, uint32_t events_) 
    : fd(_fd), events(events_) {}

void Channel::enable_events(uint32_t tar_events)
{
    auto new_events = events | tar_events;
    if(events != new_events)
    {
        events = new_events;
        update_events(this);
    }
}

void Channel::disable_events(uint32_t tar_events)
{
    auto new_events = events & ~tar_events;
    if(events != new_events)
    {
        events = new_events;
        update_events(this);
    }
}

void Channel::clear_events()
{
    if(events != 0)
    {
        events = 0;
        update_events(this);
    }
}

void Channel::handle_events(uint32_t revents)
{
    if (revents & (EPOLLIN | EPOLLRDHUP | EPOLLHUP) && trigger_read)
        trigger_read();
    
    if (revents & EPOLLOUT && trigger_send)
        trigger_send();
        
    if (revents & EPOLLERR && trigger_error)
    {
        int err_no;
        socklen_t len = sizeof(err_no);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err_no, &len);
        
        trigger_error(err_no);
    }
}

int Channel::get_fd() const { return fd; }
uint32_t Channel::get_events() const { return events; }

void Channel::activate(int fd_, uint32_t events_)
{
    fd = fd_;
    events = events_;
    int flag = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

void Channel::deactivate()
{
    trigger_read = nullptr;
    trigger_send = nullptr;
    trigger_error = nullptr;

    events = 0;
    if(fd != -1)
    {
        close(fd);
        fd = -1;
    }
}

void Channel::set_read_cb(std::function<void()> _cb) { trigger_read = std::move(_cb); }
void Channel::set_send_cb(std::function<void()> _cb) { trigger_send = std::move(_cb); }
void Channel::set_error_cb(std::function<void(int)> _cb) { trigger_error = std::move(_cb); }
void Channel::set_update_events(std::function<void(Channel*)> _cb) { update_events = std::move(_cb); }

Channel::~Channel()
{
    update_events = nullptr;
    deactivate();
}