#include "reactor/SubReactor.hpp"
#include "net/Dispatcher.hpp"

/*--------------Slot-------------------*/

Slot::Slot(uint32_t no, ConnectionInit content)
    : no(no), active(false)
{
    connection = std::make_unique<TCPConnection>(content.reactor, std::move(content._cb));
}

void Slot::register_connection(uint32_t reactor_id, Channel* channel)
{
    generations.inc();
    connection->activate(channel, ConnectionToken{reactor_id, no, generations.get()});
    active = true;
}

void Slot::unregister_connection()
{
    connection->deactivate();
    active = false;
}


/*-----------------SubReactor-----------------*/

SubReactor::SubReactor(uint32_t reactor_id, Dispatcher& dispatcher)
    : dispatcher(dispatcher), reactor_id(reactor_id)
{
    slots.reserve(INIT_CONNECTION_NUM);
    // 注册分发器端口
    dispatcher.register_reactor_port(reactor_id, [this](Operation operation){
        dispatch_operation(operation);
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
                [this](ByteStream stream){
                    dispatcher.publish_stream(stream);
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
    auto channel = try_get_channel(fd);     // 返回一个初始化好的Channel*

    if(free_slots.empty())
        create_slots();
    
    auto free_slot = free_slots.front();
    free_slots.pop();

    auto slot = slots.find(free_slot);
    if(slot != slots.end())     // 找不到说明出错了，因为slots和free_slots是同步创建的
        slot->second->register_connection(reactor_id, channel);   // 注册连接，绑定事件回调
    
}

void SubReactor::connection_close_event(const ConnectionToken& token, const CloseReason& reason, int err_no)
{
    auto slot = validate(token);
    if(slot.first)
    {
        int fd = slot.second->second->connection->get_channel()->get_fd();
        slot.second->second->unregister_connection();
        
        free_slots.push(slot.second->first);
        unregister_channel(fd);
    
        dispatcher.publish_close(token);    // 告诉业务层该连接已经关闭，可以释放对应资源
    }
}

void SubReactor::reactor_fatal_event()      // 该subreactor出现致命错误，要关闭
{
    dispatcher.unregister_reactor_port(reactor_id);
    call_main_reactor(ReactorFatalEvent{reactor_id, fatal_errno});
}

void SubReactor::send_operation(const ConnectionToken& token, std::string data)
{
    auto plot = validate(token);
    if(plot.first)
    {
        auto it = std::move(plot.second);
        it->second->connection->to_send(data.data(), data.size());
    }
}

void SubReactor::close_operation(const ConnectionToken& token)
{
    auto it = validate(token);
    if(it.first)
    {
        auto plot = std::move(it.second);
        plot->second->connection->to_close();       // 不确定可不可以关，因此只提交关闭
    }
}

void SubReactor::handle_operation(Operation operation)  // 任务分流器
{
    std::visit(overloaded{
        [this](const Send_Operation& op)
        { send_operation(op.token, op.data); },
        [this](const Close_Operation& op)
        { close_operation(op.token); }
    }, operation.payload);
}

void SubReactor::dispatch_operation(Operation operation)        // 非thread_local的线程投递入口，通过该函数完成跨线程投递
{
    run_in_loop([this, operation = std::move(operation)] {
        handle_operation(std::move(operation));
    });
}

SubReactor::target_slot SubReactor::validate(const ConnectionToken& token)      // 查找token对应的连接，返回对应的迭代器，没找到返回slots.end()
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
