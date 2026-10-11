#pragma once

#include "core/ReactorBase.hpp"

class IO_Object
{
protected:
    ReactorBase *reactor;
    Channel *channel;

    void activate_impl(Channel* channel_);
    void deactivate_impl();
    
    virtual void on_read() {}
    virtual void on_send() {}
    virtual bool on_error(int) { return false; }
    virtual void on_close(int err_no = 0) {}
    
public:
    IO_Object(ReactorBase* reactor_);

    const Channel* get_channel() const { return channel; }

    virtual ~IO_Object() = default;
};