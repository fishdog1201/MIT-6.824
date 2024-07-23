#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <grpc++/grpc++.h>

#include "mapReduce.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using mapReduce::MapReduce;
using mapReduce::TaskReply;
using mapReduce::SetTaskStatRequest;

struct TaskResponse {
    std::string fileName = "";
    int type = -1;
};

class WorkerClient {
public:
    WorkerClient(std::shared_ptr<Cahnnel> channel): stub_(WorkerClient::NewStub(channel)) {}

    TaskResponse RequestTask() {
        ClientContext context;
        Status status;
        MapTaskReply reply;
        TaskResponse reponse;

        status = stub_->AssignTask(&context, &reply);
        if (!status.ok()) {
            return reponse;
        }
        reponse.fileName = reply->fileName;
        reponse.type = reply->type;
        return reponse;
    }

    void SendTaskStat(const std::string& fileName, int type) {
        ClientContext context;
        Status status;
        SetTaskStatRequest request;

        request->set_fileName(fileName);
        request->set_type(type);

        status = stub_->SetTaskStat(&context, request);

        return;
    }

    void runWorker()
    {
        while (true) {
            TaskResponse res;
            res = RequestTask();

            switch (res.type) {
                case MAP: {
                    // TODO domaptask
                    SendTaskStat(res.fileName, res.type);
                    break;
                }
                case REDUCE: {
                    // TODO doreducetask
                    SendTaskStat(res.fileName, res.type);
                    break;
                }
                case COMPLETE: {
                    // TODO
                    std::cout << "Time to sleep\n";
                    break;
                }
                default:
                    std::cout << "WTF has happened?\n";
                    // WTF
            }
        }
    }
private:
    std::unique_ptr<WorkerClient::Stub> stub_;
    enum TASK_TYPE {
        MAP,
        REDUCE,
        COMPLETE,
    };

};

int main()
{
    WorkerClient worker(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));

   worker.runWorker();

    return 0;
}