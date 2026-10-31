#include "common.h"
using namespace ec;
int main(int argc, char** argv) {
    try {
        int arg = 1;
        auto endpoint = env("EC_ENDPOINT", "tcp://127.0.0.1:15555");
        if (argc > 2 && std::string(argv[arg]) == "--endpoint") { endpoint = argv[arg + 1]; arg += 2; }
        if (arg >= argc) throw Error("INVALID", "usage: ec_client [--endpoint URL] ping|stats|phase resize|gc status|gc run|gc pause|gc resume|put KEY VALUE|put-file KEY FILE|update KEY VALUE|update-file KEY FILE|append KEY SUFFIX|append-file KEY FILE|get KEY|delete KEY|meta KEY|repair KEY|repair-node ID");
        std::string command = argv[arg++], op = command;
        bool file = command == "put-file" || command == "update-file" || command == "append-file";
        if (file) op = command.substr(0, command.find('-'));
        if (command == "repair-node") op = "repair_node";
        for (auto& c : op) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
        Frames request{op};
        while (arg < argc) request.emplace_back(argv[arg++]);
        if ((op == "GC" || op == "PHASE") && request.size() == 2)
            for (auto& c : request[1]) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
        if (file) {
            require(request.size() == 3, "file command requires KEY FILE");
            require(fs::file_size(request[2]) <= MAX_VALUE, "file exceeds 16 MiB");
            request[2] = read_file(request[2]);
        }
        Context context;
        auto reply = rpc(context, endpoint, request, integer(env("CLIENT_TIMEOUT_MS", "120000"), 10, 3600000));
        require(reply.size() == 2, "invalid response");
        if (reply[0] != "OK") { std::cerr << reply[0] << ": " << reply[1] << '\n'; return 2; }
        if (op == "GET") std::cout.write(reply[1].data(), reply[1].size());
        else std::cout << (reply[1].empty() ? "OK" : reply[1]) << '\n';
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
