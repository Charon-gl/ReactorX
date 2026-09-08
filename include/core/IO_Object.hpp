#pragma once

#include <iostream>
#include "core/ReactorBase.hpp"

class IO_Object
{
private:
    EventReactor *_reactor;
    Channel *channel;
    bool is_active;

protected:
    void activate_impl(int fd);
    void deactive_impl(int fd);

    int fd;
    EventReactor *reactor() const noexcept;

    void enable_event(uint32_t event);
    void disable_event(uint32_t event);

    virtual void on_read() {}
    virtual void on_send() {}
    virtual bool on_error(int) {}
    virtual void on_close(int err_no = 0) {}

public:
    IO_Object(EventReactor* reactor);
    int get_fd() const;
};