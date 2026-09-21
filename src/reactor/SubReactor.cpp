#include "reactor/SubReactor.hpp"
#include "net/FrameWorkDispatcher.hpp"

Slot::Slot(uint32_t no, ConnectionInit content)
    : no(no), fd(-1), active(false)
{
    connection = std::make_unique<TCPConnection>(content.reactor, std::move(content._cb));
}

void Slot::register_connection(uint32_t reactor_id, Channel* channel)
{
    fd = channel->get_fd();
    generations.inc();
    connection->activate(channel, ConnectionToken{reactor_id, no, generations.get()});
    active = true;
}

void Slot::unregister_connection()
{
    fd = -1;
    connection->deactivate();
    active = false;
}


/*-----------------SubReactor-----------------*/

SubReactor::SubReactor(uint32_t reactor_id, FrameWorkDispatcher& dispatcher)
    : dispatcher(dispatcher), reactor_id(reactor_id)
{
    slots.reserve(INIT_CONNECTION_NUM);
    // 注册分发器端口
    dispatcher.register_reactor_port(reactor_id, [this](Reactor_Command task){
        dispatch_command(task);
    });

    create_slots();   
}

void SubReactor::create_slots()
{
    slots.reserve(slots.size() + INIT_CONNECTION_NUM);
    for(int i = 0; i < INIT_CONNECTION_NUM; ++i)
    {
        auto it = std::make_unique<Slot>(reactor_counter.get(), 
            ConnectionInit{this, 
                [this](Request task){
                    dispatcher.publish_request(task);
                }
            }
        );
        free_slots.push(it->no);
        slots.emplace(it->no, std::move(it));

        reactor_counter.inc();
    }
}

void SubReactor::new_connection_event(int fd)
{
    auto channel = std::make_unique<Channel>(fd);

    if(free_slots.empty())
        create_slots();
    
    auto free_slot = free_slots.front();
    free_slots.pop();

    auto slot = slots.find(free_slot);
    if(slot != slots.end())     // 找不到说明出错了，因为slots和free_slots是同步创建的
        slot->second->register_connection(reactor_id, channel.get());   
    
    register_channel(std::move(channel));
}

void SubReactor::connection_close_event(const ConnectionToken& token, const CloseReason& reason, int err_no)
{
    auto slot = validate(token);
    if(slot.first)
    {
        int fd = slot.second->second->fd;
        slot.second->second->unregister_connection();
        unregister_channel(fd);
        free_slots.push(slot.second->first);
    }
}

void SubReactor::reactor_fatal_event(int err_no)
{
    stop_epoll(err_no);
    dispatcher.unregister_reactor_port(reactor_id);
}

void SubReactor::response_command(const ConnectionToken& token, std::string data)
{
    auto plot = validate(token);
    if(plot.first)
    {
        auto it = std::move(plot.second);
        it->second->connection->to_send(data.data(), data.size());
    }
}

void SubReactor::close_command(const ConnectionToken& token)
{
    auto plot = validate(token);
    if(plot.first)
    {
        auto it = std::move(plot.second);
        it->second->connection->to_close();
    }
}

void SubReactor::handle_command(Reactor_Command command)
{
    std::visit(overloaded{
        [this](const Response_Command& c)
        { response_command(c.token, c.data); },
        [this](const Close_Command& c)
        { close_command(c.token); }
    }, command.payload);
}

void SubReactor::dispatch_command(Reactor_Command command)
{
    run_in_loop([this, command = std::move(command)] {
        handle_command(std::move(command));
    });
}

SubReactor::target_slot SubReactor::validate(const ConnectionToken& token)
{
    auto it = slots.find(token.slot);
    if(it != slots.end())
    {
        if(it->second->connection->get_token() == token)
            return target_slot(true, std::move(it));
    }
    return target_slot(false, slots.end());
}

void SubReactor::set_call_main_reactor(std::function<void(const ReactorFatalEvent&)> _cb) { call_main_reactor = std::move(_cb); }

SubReactor::~SubReactor()
{
    call_main_reactor(ReactorFatalEvent{reactor_id, fatal_errno});
}