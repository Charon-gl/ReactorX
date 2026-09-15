#include "core/IO_Object.hpp"


IO_Object::IO_Object(ReactorBase* reactor)
    : _reactor(reactor), channel(nullptr), is_active(false) {}

void IO_Object::activate_impl(int _fd)
{
    auto it = std::make_unique<Channel>(_fd);
    fd = _fd;

    it->set_read_cb([this] { 
        on_read(); 
    });
    it->set_send_cb([this] { 
        on_send(); 
    });
    it->set_error_cb([this](int err_no) { 
        return on_error(err_no); 
    });

    channel = it.get();
    _reactor->register_channel(std::move(it));
    is_active = true;
}

void IO_Object::unvaild() { channel->unvaild(); }
bool IO_Object::is_vaild() const { return channel->get_vaild(); }

void IO_Object::deactive_impl(int fd)
{
    channel = nullptr;
    _reactor = nullptr;
    is_active = false;
}

ReactorBase *IO_Object::reactor() const noexcept { return _reactor; }

void IO_Object::enable_event(uint32_t event) { channel->enable_events(event); }
void IO_Object::disable_event(uint32_t event) { channel->disable_events(event); }

int IO_Object::get_fd() const { return channel->get_fd(); }