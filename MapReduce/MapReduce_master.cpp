#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
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
    Status AssignMapTask(ServerContext* context, MapTaskReply* reply) override
    {
        if (finishedMapTasks.size() == fileNum) {
            reply->set_MapFileName("empty");
        }
        if (!fileList.empty()) {
            std::lock_guard<std::mutex> lck(mtx); 
            std::string task = fileList.back();
            fileList.pop_back();
            mtx.unlock();
            AddTaskToRunningMapTasks(task);
            return std::string(task);
        }

        return "empty";

    }
    Status AssignReduceTask(ServerContext* context, MapTaskReply* reply) override {}
    Status SetMapTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override {}
    Status SetReduceTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override {}

    void GetAllFiles(int argc, char* argv[])
    {
        for (int i = 1; i < argc; i++) {
            fileList.emplace_back(argv[i]);
        }
        fileNum = argc - 1;
    }

    void AddTaskToRunningMapTasks(const std::string& task) {}
private:
    int fileNum;
    std::vector<std::string> fileList;
    std::vector<std::string> runningMapTasks;
    std::vector<int> runningReduceTasks;
    std::unordered_map<std::tring, int> finishedMapTasks;
    std::unordered_map<int, int> finishedReduceTasks;
    std::mutex mtx;
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