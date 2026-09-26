#include "reactor/MainReactor.hpp"
#include "net/Acceptor.hpp"

Acceptor::Acceptor(MainReactor* mainreactor) : IO_Object(mainreactor), port(0)
{
    memset(&addr, 0, sizeof(addr));
}

std::unique_ptr<Channel> Acceptor::active(uint16_t port_)
{
    port = port_;
    int fd = init_listen_fd();
    if(fd == -1)
        return nullptr;

    auto it = std::make_unique<Channel>(fd);

    activate_impl(it.get());

    return std::move(it);
}

void Acceptor::deactive()
{
    memset(&addr, 0, sizeof(addr));
    deactive_impl();
}

int Acceptor::init_listen_fd()
{
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd == -1)
    {
        std::cerr << "Socket failed" << std::endl;
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int ret = bind(lfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (ret == -1)
    {
        std::cerr << "Bind failed" << std::endl;
        close(lfd);
        return -1;
    }

    ret = listen(lfd, MAX_LISTEN_NUM);
    if (ret == -1)
    {
        std::cerr << "Listen failed" << std::endl;
        close(lfd);
        return -1;
    }

    int flat = fcntl(lfd, F_GETFL);
    fcntl(lfd, F_SETFL, flat | O_NONBLOCK);

    return lfd;
}

void Acceptor::on_read()
{
    sockaddr_in caddr;
    memset(&caddr, 0, sizeof(caddr));
    socklen_t len = sizeof(caddr);

    while (true)
    {
        int cfd = accept(channel->get_fd(), reinterpret_cast<sockaddr*>(&caddr), &len);
        if (cfd == -1)
        {
            bool res = on_error(errno);
            if(!res)
                break;
        }
        // 投递新连接
        static_cast<MainReactor*>(reactor)->round_dispatch(cfd); // 可以考虑扩展成批量投递
    }
}

bool Acceptor::on_error(int err_no)
{
    auto err_rank = Err_Manager::err_judge(err_no);
    switch (err_rank)
    {
    case Err_Rank::RETRY: // 非阻塞io不会EINTR，这里直接忽略即可
        return true;
    case Err_Rank::IGNORE:
        break;
    default:
     // 如果是延迟关闭，那就要先注销读监听，避免在删除前来读事件；其实这里更推荐直接删掉
        on_close(err_no);
        break;
    }
    return false;
}

void Acceptor::on_close(int err_no)
{
    static_cast<MainReactor*>(reactor)->remove_listener(channel->get_fd(), get_reason(err_no), err_no);
}

Acceptor::~Acceptor()
{
    deactive();
}