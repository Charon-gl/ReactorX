#pragma once

#include <iostream>
#include "core/ReactorBase.hpp"

class IO_Object
{
protected:
    ReactorBase *reactor;
    Channel *channel;

    void activate_impl(Channel* channel_);
    void deactive_impl();
    
    virtual void on_read() {}
    virtual void on_send() {}
    virtual bool on_error(int) {}
    virtual void on_close(int err_no = 0) {}
    
public:
    IO_Object(ReactorBase* reactor_);

    virtual ~IO_Object() = default;
};