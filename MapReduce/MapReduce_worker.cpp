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
using mapReduce::MapTaskReply;
using mapReduce::SetMapTaskStatRequest;
using mapReduce::SetReduceTaskStatRequest;
using mapReduce::GetMapNumReply;
using mapReduce::GetReduceNumReply;

class WorkerClient {
public:
    WorkerClient(std::shared_ptr<Cahnnel> channel): stub_(WorkerClient::NewStub(channel)) {}

    std::string RequestMapTask() {
        ClientContext context;
        Status status;
        MapTaskReply reply;

        status = stub_->AssignMapTask(&context, &reply);
        if (status.ok()) {
            return reply.MapFileName();
        }

        return "";
    }

    void SendMapTaskStat() {
        ClientContext context;
        Status status;
        SetMapTaskStatRequest request;

        status = stub_->SetMapTaskStat(&context, request);

        return;
    }
private:
    std::unique_ptr<WorkerClient::Stub> stub_;
};

int main()
{
    WorkerClient worker(grpc::CreateChannel("localhost:50001", grpc::InsecureChannelCredentials()));

    return 0;
}