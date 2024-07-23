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
using mapReduce::TaskReply;
using mapReduce::SetTaskStatRequest;


class MasterImpl final : public MapReduce::Service
{
public:
    MasterImpl() {}

    Status AssignTask(ServerContext* context, TaskReply* reply) override
    {
        if (!mapFileList.empty()) {
            std::lock_guard<std::mutex> lck(mtx); 
            std::string task = fileList.back();
            fileList.pop_back();
            mtx.unlock();
            waitMap(task);
            reply->set_FileName(task);
            reply->set_type(MAP);
            return Status::OK;
        }
        if (finishedMapTasks.size() == fileNum && !reduceFileList.empty()) {
            //TODO: assign reduce task
            return Status::OK;
        }
        reply->set_FileName("");
        reply->set_type(COMPLETE);
    }

    Status SetTaskStat(ServerContext* context, SetTaskStatRequest* request) override
    {
        std::lock_guard<std::mutex> lck(mtx);
        if (request->type == MAP) {
            finishedMapTasks[request->fileName] = 1;
            runningMapTasks.remove(request->fileName);
        } else if (request->type == REDUCE) {
            finishedReduceTasks[request->fileName] = 1;
            runningReduceTasks.remove(request->fileName);
        }
        return Status::OK;
    }

    void GetAllMapFiles(int argc, char* argv[])
    {
        for (int i = 1; i < argc; i++) {
            mapFileList.emplace_back(argv[i]);
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
            runningMapTasks.remove(task);
        }
        std::cout << "Map task finished, filename is " << task << std::endl;
    }
private:
    int fileNum;
    int mapNum;
    int reduceNum;
    std::vector<std::string> mapFileList;
    std::vector<std::string> reduceFileList;
    std::vector<std::string> runningMapTasks;
    std::vector<std::string> runningReduceTasks;
    std::unordered_map<std::string, int> finishedMapTasks;
    std::unordered_map<std::string, int> finishedReduceTasks;
    std::mutex mtx;
    enum TASK_TYPE {
        MAP,
        REDUCE,
        COMPLETE,
    };
};

int main(int argc, char* argv[])
{
    if(argc < 2){
        std::cout << "missing parameter! The format is ./Master pg*.txt" << std::endl;
        exit(-1);
    }

    std::string server_address("0.0.0.0:50001");
    MasterImpl service;

    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server is listening on " << server_address << std::endl;

    service.GetAllMapFiles(argc, argv);

    server->Wait();

    return 0;
}