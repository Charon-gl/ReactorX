#include <iostream>
#include <fstream>

class Logger
{
public:
    enum class LOG_RANK
    {
        DEBUG = 0,
        INFO,
        //WARNING,
        ERROR,
        FATAL
    };

    void add_log(LOG_RANK, const char*, int _errno, int _fd = -1);
    static Logger &instance(const std::string &);

    Logger(const Logger &) = delete;
    Logger(Logger &&) = delete;

    ~Logger();

private:
    Logger(const std::string&);
    std::ofstream log_file;
};