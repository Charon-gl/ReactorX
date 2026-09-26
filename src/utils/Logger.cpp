#include "utils/Logger.hpp"

Logger::Logger(const std::string& _log_file_) 
{ 
    log_file.open(_log_file_, std::ios::out);
    if(!log_file.is_open())
        std::cerr << "Failed to open log_file" << std::endl;
}

Logger &Logger::instance(const std::string &_log_file_)
{
    static Logger logger(_log_file_);
    return logger;
}

void Logger::add_log(LOG_RANK rank, const char* log_str, int _errno, int _fd)
{
    if(rank == LOG_RANK::DEBUG)
        log_file << "DEBUG" << _errno << ": " << _fd << " " << log_str << std::endl;
    else if(rank == LOG_RANK::INFO)
        log_file << "INFO" << ": " << "连接" << _fd << " " << log_str << std::endl;
    else if (rank == LOG_RANK::ERROR)
        log_file << "ERROR" << _errno << ": " << "连接" << _fd << " " << log_str << std::endl;
    else if(rank == LOG_RANK::FATAL)
        log_file << "FATAL" << _errno << ": " << log_str << std::endl;

    return;
}

Logger::~Logger() { log_file.close(); }