#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <functional>
#include <chrono>
#include <grpc++/grpc++.h>

#include "MapReduce.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mapReduce::MapReduce;
using mapReduce::MapTaskReply;
using mapReduce::SetMapTaskStatRequest;
using mapReduce::SetReduceTaskStatRequest;
using mapReduce::GetMapNumReply;
using mapReduce::GetReduceNumReply;


class MasterImpl final : public MapReduce::Service
{
public:
    MasterImpl() {}

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
            waitMap(task);
            reply->set_MapFileName(task);
        }

        reply->set_MapFileName("empty");
    }

    Status SetMapTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override 
    {
        std::lock_guard<std::mutex> lck(mtx);
        finishedMapTasks[request->MapFileName] = 1;
        return;
    }

    Status GetMapNum(ServerContext* context, GetMapNumReduceReply* reply) override
    {
        reply->set_mapNum(mapNum);
    }

    Status GetMapNum(ServerContext* context, GetReduceNumReduceReply* reply) override
    {
        reply->set_reduceNum(reduceNum);
    }

    Status AssignReduceTask(ServerContext* context, MapTaskReply* reply) override {}
    Status SetReduceTaskStat(ServerContext* context, SetMapTaskStatRequest* request) override {}

    void GetAllFiles(int argc, char* argv[])
    {
        for (int i = 1; i < argc; i++) {
            fileList.emplace_back(argv[i]);
        }
        fileNum = argc - 1;
    }

    void waitMap(const std::string& task)
    {
        std::lock_guard<std::mutex> lck(mtx);
        runningMapTasks.emplace_back(task);
        mtx.unlock();
        std::thread waitMap_thread(std::bind(&MasterImpl::waitMapTask, this, task));
        waitMap_thread.detach();
    }

    void waitMapTask(const std::string& task)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(8000));
        std::lock_guard<std::mutex> lck(mtx);
        if (finishedMapTasks.count(task) == 0) {
            std::cout << "Map task timeout, filename is " << task << std::endl;
            fileList.emplace_back(task);
        }
        std::cout << "Map task finished, filename is " << task << std::endl;
    }
private:
    int fileNum;
    int mapNum;
    int reduceNum;
    std::vector<std::string> fileList;
    std::vector<std::string> runningMapTasks;
    std::vector<int> runningReduceTasks;
    std::unordered_map<std::string, int> finishedMapTasks;
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