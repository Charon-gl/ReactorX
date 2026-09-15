#pragma once

#include <iostream>
#include "core/ReactorBase.hpp"

class IO_Object
{
private:
    ReactorBase *_reactor;
    Channel *channel;
    bool is_active;

protected:
    void activate_impl(int fd);
    void deactive_impl(int fd);

    int fd;
    ReactorBase *reactor() const noexcept;

    void disable_event(uint32_t event);
    
    virtual void on_read() {}
    virtual void on_send() {}
    virtual bool on_error(int) {}
    virtual void on_close(int err_no = 0) {}
    
public:
    IO_Object(ReactorBase* reactor);

    void unvaild();       // 将channel的is_heard设为false，并注销读监听
    bool is_vaild() const;        // 返回channel的vaild值

    int get_fd() const;
    void enable_event(uint32_t event);
};