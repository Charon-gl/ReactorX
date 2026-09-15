#include "http/http_server.hpp"


int main(int argc, char** argv)
{
    http_server server(static_cast<uint16_t>(6666), 1);
    server.run();


    return 0;

}