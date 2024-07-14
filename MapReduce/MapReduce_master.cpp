#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

#include "MapReduce.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mapReduce::MapReduce


class MasterImpl final : public MapReduce::Service
{
    Status AssignTask(ServerContext* context, TaskReply* reply) override
    {
        
    }
};

int main(int argc, char* argv[])
{
    if(argc < 2){
        cout << "missing parameter! The format is ./Master pg*.txt" << endl;
        exit(-1);
    }
}