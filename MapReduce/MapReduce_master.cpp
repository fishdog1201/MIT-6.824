#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <grpc++/grpc++.h>

#include "MapReduce.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mapReduce::MapReduce;
using mapReduce::MapTaskRequest;
using mapReduce::MapTaskReply;
using mapReduce::SetMapTaskStatRequest;
using mapReduce::SetReduceTaskStatRequest;


class MasterImpl final : public MapReduce::Service
{
public:
    Status AssignMapTask(ServerContext* context, MapTaskReply* reply) override {}
    Status AssignReduceTask(ServerContext* context, MapTaskReply* reply) override {}
    Status SetMapTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override {}
    Status SetReduceTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override {}

    void GetAllFiles(int argc, char* argv[]) {}
private:
    int fileNum;
    std::vector<std::string> fileList;
    std::vector<std::string> runningMapTask;
    std::vector<int> runningReduceTask;
    std::unordered_map<std::tring, int> finishedMapTask;
    std::unordered_map<int, int> finishedReduceTask;
};

void RunServer() {
    std::string server_address("0.0.0.0:50001");
    MasterImpl service;

    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server is listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char* argv[])
{
    if(argc < 2){
        std::cout << "missing parameter! The format is ./Master pg*.txt" << std::endl;
        exit(-1);
    }

    RunServer();

    return 0;
}