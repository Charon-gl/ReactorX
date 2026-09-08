#include "utils/Err_Manager.hpp"

Err_Rank Err_Manager::err_judge(int err_no)
{
    switch (err_no)
    {
    case EAGAIN:
    case ECONNABORTED:
    // case EMFILE:
    case EEXIST:
        return Err_Rank::IGNORE;

    case EINTR:
        return Err_Rank::RETRY;

    case ETIMEDOUT:
    case EPIPE:
    case EBADF:
    case ENOENT:
    case ECONNRESET:
    case ENETUNREACH:
    case EHOSTUNREACH:
    case ENOSPC:
        return Err_Rank::CLOSE_CONNECTION;

    case ENFILE:
    case ENOBUFS:
    case ENOMEM:
        return Err_Rank::FATAL;
    default:
        return;
    }
}