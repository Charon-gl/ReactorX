#include "core/Channel.hpp"

Channel::Channel(int _fd) 
    : fd(_fd), events(EPOLLET), valid(true)
{
    int flag = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

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
    
    if (revents & EPOLLIN && trigger_read)
        trigger_read();
    
    if (revents & EPOLLOUT && trigger_send)
        trigger_send();
        
    if ((revents & EPOLLRDHUP || revents & EPOLLHUP) && trigger_read)
        trigger_read();

    if (revents & EPOLLERR)
    {
        int err_no;
        socklen_t len = sizeof(err_no);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err_no, &len);
        
        if(trigger_error)
            trigger_error(err_no);
    }
}

int Channel::get_fd() const { return fd; }
uint32_t Channel::get_events() const { return events; }

bool Channel::get_vaild() const { return valid.load(std::memory_order_seq_cst); }
void Channel::unvaild()
{
    auto res = valid.load(std::memory_order_seq_cst);
    if(res)
    {
        valid.store(false);
        disable_events(EPOLLIN);
    }
}

void Channel::set_read_cb(std::function<void()> _cb) { trigger_read = std::move(_cb); }
void Channel::set_send_cb(std::function<void()> _cb) { trigger_send = std::move(_cb); }
void Channel::set_error_cb(std::function<void(int)> _cb) { trigger_error = std::move(_cb); }
void Channel::set_update_events(std::function<void(Channel*)> _cb) { update_events = std::move(_cb); }

Channel::~Channel()
{
    if(fd != -1)
        close(fd);
    fd = -1;
}