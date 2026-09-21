#include "core/IO_Object.hpp"


IO_Object::IO_Object(ReactorBase* reactor_)
    : reactor(reactor_), channel(nullptr) {}

void IO_Object::activate_impl(Channel* channel_)
{
    channel = channel_;
    channel->set_read_cb([this] { 
        on_read(); 
    });
    channel->set_send_cb([this] { 
        on_send(); 
    });
    channel->set_error_cb([this](int err_no) { 
        return on_error(err_no); 
    });
}

void IO_Object::deactive_impl()
{
    channel = nullptr;
}
