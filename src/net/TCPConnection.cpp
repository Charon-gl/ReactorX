#include "reactor/SubReactor.hpp"
#include "net/TCPConnection.hpp"

std::unordered_map<Conn_state, std::unordered_set<Conn_state>> transition_table = {
    {Conn_state::UNCONNECT, {Conn_state::CONNECTED}},
    {Conn_state::CONNECTED, {Conn_state::PROCESSING, Conn_state::CLOSING}},
    {Conn_state::PROCESSING, {Conn_state::CONNECTED, Conn_state::CLOSING}},
    {Conn_state::CLOSING, {Conn_state::UNCONNECT}}
};

bool is_valid_transition(Conn_state cur_state, Conn_state new_state)        //判断状态转换是否合法
{
    return transition_table[cur_state].count(new_state);
}

TCPConnection::TCPConnection(SubReactor* reactor) 
    : IO_Object{reactor}, send_begin_pos(0), state{Conn_state::UNCONNECT}
{    
    recv_buf.reserve(MAX_BUF_SIZE);
    send_buf.reserve(MAX_BUF_SIZE);
}

bool TCPConnection::transition(Conn_state new_state)
{
    Conn_state cur = state.load(std::memory_order_seq_cst);
    if(!is_valid_transition(cur, new_state))
        return false;

    while(!state.compare_exchange_strong(cur, new_state, std::memory_order_seq_cst))
        ;
    return true;
}

void TCPConnection::on_read()
{
    Conn_state cur = state.load(std::memory_order_seq_cst);
    if (cur == Conn_state::UNCONNECT || cur == Conn_state::CLOSING)
        return;

    while(true)
    {
        size_t n = recv_buf.size();
        recv_buf.resize(n + MAX_BUF_SIZE);
        int res = read(fd, recv_buf.data() + n, recv_buf.capacity() - n);
        if(res == 0)
        {// 一种是正处于processing态，关闭读监听，继续后面的流程；一种是处于connected状态，可以走关闭流程
            switch (cur)
            {
            case Conn_state::PROCESSING:
                unvaild();
                return;
            default:
                on_close();
                return;
            }
        }
        else if (res == -1)
        {
            recv_buf.resize(n);
            bool res = on_error(errno);
            if(!res)
                break;
            return;
        }

        // 接收到了，手动更新size
        recv_buf.resize(n + res);
    }

    request_callback();
    return;
}

void TCPConnection::on_send()
{
    auto cur = state.load(std::memory_order_seq_cst);
    if (cur == Conn_state::UNCONNECT || cur == Conn_state::CLOSING)
        return;

    size_t n = send_buf.size();
    while (!send_buf.empty())
    {
        int res = send(fd, send_buf.data() + send_begin_pos, n - send_begin_pos, 0);
        if(res > 0)
            send_begin_pos += res;
        else if(res < 0)
        {
            on_error(errno);
            return;
        }
        else
        {
            transition(Conn_state::CLOSING);
            on_close();
            return;
        }
    }
    // 执行到这意味着当前写缓冲区的数据已全部发送完毕，可以给写缓冲区清空数据
    send_buf.clear();
    disable_event(EPOLLOUT);

    if(!is_vaild())     // vaild为false，说明连接可以关闭了
    {
        on_close();
    }

    return;
}

bool TCPConnection::on_error(int err_no)
{
    auto err_rank = Err_Manager::err_judge(err_no);
    switch (err_rank)
    {
    case Err_Rank::IGNORE:
    case Err_Rank::RETRY:       // 非阻塞io不会EINTR，这里直接忽略即可
        return false;
    default:
        on_close(err_no);
    }
    return true;
}

void TCPConnection::on_close(int err_no)
{// 若此时仍在处理业务，要告知业务层该连接即将关闭，不要再访问该TCPConnection
    if(state.load(std::memory_order_seq_cst) == Conn_state::PROCESSING)
        close_callback(this, err_no);

    unvaild();
    CloseReason res = get_reason(err_no);
    static_cast<SubReactor *>(reactor())->dispatch_event(ConnectionCloseEvent{fd, res, err_no});
}

void TCPConnection::process_close(int err_no)
{
    CloseReason res = get_reason(err_no);
    static_cast<SubReactor *>(reactor())->dispatch_event(ConnectionCloseEvent{fd, res, err_no});
}

void TCPConnection::activate(int fd)
{
    recv_buf.reserve(MAX_BUF_SIZE);
    send_buf.reserve(MAX_BUF_SIZE);
    transition(Conn_state::CONNECTED);

    activate_impl(fd);
}

void TCPConnection::deactivate()
{
    transition(Conn_state::UNCONNECT);
    recv_buf.clear();
    send_buf.clear();
    send_begin_pos = 0;

    deactive_impl(fd);
}

Conn_state TCPConnection::get_state() const { return state.load(); }

const char *TCPConnection::peek() const { return recv_buf.data(); }

size_t TCPConnection::readable_bytes() const { return recv_buf.size(); }

void TCPConnection::consume(size_t len)
{
    if(state.load(std::memory_order_seq_cst) == Conn_state::PROCESSING)
    {
        if(len > recv_buf.size() || len <= 0)
            return;
        recv_buf.erase(0, len);
    }
}

void TCPConnection::clear_recvbuf()
{
    recv_buf.clear();
}

void TCPConnection::append_buf(const char *data, size_t len)
{
    if(state.load(std::memory_order_seq_cst) == Conn_state::PROCESSING)
    {
        if(data == nullptr)
            return;
        send_buf.append(data + send_buf.size(), len);
    } 
}

size_t TCPConnection::sendable_bytes() const { return send_buf.size() - send_begin_pos; }       // 非线程安全

void TCPConnection::set_request_callback(Call_Process _cb) { request_callback = std::move(_cb); }

void TCPConnection::set_close_callback(Call_Connection_Close _cb) { close_callback = std::move(_cb); }

void TCPConnection::enter_processing()
{
    if(state.load(std::memory_order_seq_cst) != Conn_state::CONNECTED)
        return;
    transition(Conn_state::PROCESSING);
}

void TCPConnection::exit_processing()
{
    if (state.load(std::memory_order_seq_cst) != Conn_state::PROCESSING)
        return;
    transition(Conn_state::CONNECTED);
}

TCPConnection::~TCPConnection()
{
    request_callback = nullptr;
    close_callback = nullptr;
}

/*对于多线程/协程的业务处理，需要考虑安全问题，
由于某个连接在进行业务处理的过程中连接可能被突然释放掉，
这时候当业务处理完成调用回调返回时会造成非法访问，在此先提前写下一个思路：

业务处理返回给tcpconnection主要是通过回调或者调用的方式将约为结果存放到tcpconnection的写缓冲区内，
这种方式需要sharedptr和weakptr的使用保证线程安全。但是这就需要去改变现有的底层架构。
我们依然可以通过跨线程投递的方式，业务处理完成后调用一个回调，将业务结果打包成一个任务投回
subreactor的任务队列，然后再由subreactor将其写入到对应的tcpconnection中。

但是这依旧解决不了tcpconnection被另一个连接复用的问题
我们可以设定一个机制：当这个tcpconnection在进行业务处理时，
若连接将被关闭，则让subreactor向业务线程发送该连接已失效的标志位。
业务线程在打包任务时要传入这个标志位以及对应的fd。
当subreactor拿到后检测fd和标志位，只要标志位为false，丢弃该包
只有fd存活且标志位为true时才将该包写入对应的tcpconnection的写缓冲区。

当然这种方式会使得处理一条连接的时间变长。
*/