#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <ctime>
#include <unistd.h>
#include <queue>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>

#ifdef BAZEL_BUILD
#include "examples/protos/grpcHandler.grpc.pb.h"
#else
#include "grpcHandler.grpc.pb.h"
#endif

using namespace std;
using grpc::Alarm;
using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpcHandler::GrpcHandler;
using grpcHandler::NdrResponse;

int hoPrepCount = 0;

class GrpcHandlerClient
{

   enum class ClientStatus
   {
      READ = 1,
      WRITE = 2,
      CONNECT = 3,
      WRITES_DONE = 4,
      WAITING_TO_WRITE = 5,
      FINISH = 6
   };

   // struct for keeping state and data information
   struct AsyncClientCall
   {
      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // Finish status when the client is done with the stream.
      Status finish_status_ = Status::OK;

      // Status of client
      ClientStatus status;

      // queue of messages to be sent
      std::queue<std::string> localQueue;

      // Container for the data we send to the server.
      NdrResponse request;

      // Container for the data we expect from the server.
      NdrResponse reply;

      // Uniquely identify the message
      std::string mac;

      bool serverConnected = false;

      int thresholdCount;

      time_t waitingStartTime;

      std::unique_ptr<Alarm> alarm_;

      // The bidirectional, asynchronous stream for sending/receiving messages.
      std::unique_ptr<ClientAsyncReaderWriter<NdrResponse, NdrResponse>> stream_;
   };

public:
   // unordered_map<int, AsyncClientCall *> hashMap;
   std::queue<AsyncClientCall *> contextQueue;

   bool serverConnected;

   explicit GrpcHandlerClient(std::shared_ptr<Channel> channel, int streamCount)
       : stub_(GrpcHandler::NewStub(channel))
   {
      // Is server up before we send the RPC?
      std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() + std::chrono::hours(24);
      auto state = channel->GetState(true);
      while (state != GRPC_CHANNEL_READY)
      {
         if (!channel->WaitForStateChange(state, _deadline))
         {
            serverConnected = false;
         }
         state = channel->GetState(true);
      }

      serverConnected = true;

      if (serverConnected) {
         establishConnectionToStreams(streamCount);
      }
   }

   // returns the next AsyncClientCall
   AsyncClientCall* getNextContext()
   {
      AsyncClientCall *call = contextQueue.front();
      contextQueue.pop();
      contextQueue.push(call);
      return call;
   }

   // creates and stores AsyncClientCalls
   void establishConnectionToStreams(int streamCount)
   {
      for (int i = 1; i <= streamCount; i++)
      {
         // create new AsynceClientCall
         AsyncClientCall *call = new AsyncClientCall();
         call->status = ClientStatus::CONNECT;
         call->stream_ = stub_->AsyncSayHelloNew(&call->context, &cq_, (void *)call);
         contextQueue.push(call);
         // TODO - need to add to hashMap instead of queue
         // hashMap[i] = call;
      }
   }

   // Adds a message from given mac address to given call's local queue
   void sendToQueue(std::string mac, int startNum, AsyncClientCall *call)
   {
      call->mac = mac + std::to_string(startNum); //+ std::to_string(i);

      std::string text;
      // std::stringstream ss;
      // ss << call;
      text = "testMessage from : " + mac + std::to_string(startNum); //+ std::to_string(i);
      call->localQueue.push(text);
   }

   // Loop while listening for completed responses.
   // Prints out the response from the server.
   void AsyncCompleteRpc(std::shared_ptr <grpc::Channel> channelName)
   {
      void *got_tag;
      bool ok = false;

      // Block until the next result is available in the completion queue "cq".
      while (true)
      {
         // Verify that the request was completed successfully. Note that "ok"
         // corresponds solely to the request for updates introduced by Finish().
         GPR_ASSERT(cq_.Next(&got_tag, &ok));
         //GPR_ASSERT(ok);

         // The tag in this example is the memory location of the call object
         AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

         // ensure the client can send to the server
         auto state = channelName->GetState(true);
         if (state != GRPC_CHANNEL_READY)
         {
            std::cout << "Connection to server lost." << std::endl;
            // loop until the connection is re-established
            while (channelName->GetState(true) != GRPC_CHANNEL_READY)
            {

            }
            std::cout << "Connection to server re-established." << std::endl;

            // re-establsih streams with server
            int size = contextQueue.size();
            while (contextQueue.size() > 0)
            {
               contextQueue.pop();
               // TODO - delete call
            }
            std::cout << "Re-establishing " << size << " streams with server." << std::endl;
            // TODO - this should be a configurable parameter, instead of using contextQueue.size()
            establishConnectionToStreams(size);
         }

         // It's important to process all tags even if the ok is false. One might
         // want to deallocate memory that has be reinterpret_cast'ed to void*
         // when the tag got initialized. For our example, we cast an int to a
         // void*, so we don't have extra memory management to take care of.
         if (ok)
         {
            std::cout << "**** Call Tag: " << got_tag << std::endl;
            switch (call->status)
            {
               case ClientStatus::CONNECT:
                  std::cout << "   CONNECT" << std::endl;
                  std::cout << "   Stream connected." << std::endl;
                  call->serverConnected = true;
                  call->status = ClientStatus::WRITE;
                  PutTaskBackToQueue(call);
                  break;
               case ClientStatus::READ:
                  std::cout << "   READ" << std::endl;
                  if (call->reply.message() == "")
                  {
                     // start reading messages from the server
                     call->stream_->Read(&call->reply, (void *)call);
                  }
                  else if (call->reply.message() == "WRITES_DONE")
                  {
                     // server is done writing messages, client can write
                     call->thresholdCount++;
                     call->request.clear_message(); // TODO - maybe don't need to clear this
                     call->status = ClientStatus::WRITE;
                     PutTaskBackToQueue(call);
                  }
                  else
                  {
                     std::cout << "   Read a new message:" << call->reply.message() << std::endl;
                     // process the message from the server
                     hoPrepCount++;
                     call->waitingStartTime = time(0);
                     call->thresholdCount = 0;
                     // continue reading messages from server
                     // call->reply.clear_message();
                     call->stream_->Read(&call->reply, (void *)call);
                  }
                  break;
               case ClientStatus::WRITE:
                  std::cout << "   WRITE" << std::endl;
                  if (!call->localQueue.empty())
                  {
                     call->thresholdCount = 0;
                     call->waitingStartTime = time(0);
                     // send a message from the queue, then read
                     std::cout << "   Sending message: " << call->localQueue.front() << std::endl;
                     call->request.set_message(call->localQueue.front());
                     call->localQueue.pop();
                     call->status = ClientStatus::READ;
                     call->stream_->Write(call->request, (void *)call);
                  }
                  else
                  {
                     // if (call->reply.message() == "WRITES_DONE") {
                     //    call->thresholdCount++;
                     // }
                     std::cout << "Time spent waiting: " << time(0) - call->waitingStartTime << endl;
                     if ((time(0) - call->waitingStartTime) < 5) {
                        // notify the server that the client is done sending
                        std::cout << "   Sending message: WRITES_DONE" << std::endl;
                        call->request.set_message("WRITES_DONE");
                        call->status = ClientStatus::WRITES_DONE;
                        call->stream_->Write(call->request, (void *)call);
                     } else {
                        // threshold of WRITES_DONE messages has been met, wait for message to send
                        std::cout << "   Threshold met." << std::endl;
                        call->reply.clear_message();
                        // TODO - instead of threshold of messages, this should be a 5 sec timer
                        call->thresholdCount = 0;
                        call->status = ClientStatus::WAITING_TO_WRITE;
                        PutTaskBackToQueue(call);
                     }
                  }
                  break;
               case ClientStatus::WRITES_DONE:
                  std::cout << "   WRITES_DONE" << std::endl;
                  // read messages from the server
                  call->status = ClientStatus::READ;
                  call->stream_->Read(&call->reply, (void *)call);
                  break;
               case ClientStatus::WAITING_TO_WRITE:
                  std::cout << "   WAITING_TO_WRITE" << std::endl;
                  std::cout << "HO PREP's received: " << hoPrepCount << std::endl;
                  // check the localQueue until there is a message to write
                  while (call->localQueue.empty())
                  {

                  }
                  // message has entered to queue, go to WRITE
                  std::cout << "   going to WRITE" << std::endl;
                  call->waitingStartTime = time(0);
                  call->status = ClientStatus::WRITE;
                  PutTaskBackToQueue(call);
                  break;
               case ClientStatus::FINISH:
                  std::cout << "   FINISH" << std::endl;
                  std::cout << "   Streaming complete for Call Tag: " << got_tag << std::endl;
                  // TODO - actually delete call on FINISH
                  // delete call;
                  break;
               default:
                  std::cout << "   ERROR: Unrecognized status." << std::endl;
            }
         }
      }
   }

private:
   // Similar to the async hello example in greeter_async_client but does not
   // wait for the response. Instead queues up a tag in the completion queue
   // that is notified when the server responds back (or when the stream is
   // closed). Returns false when the stream is requested to be closed.
   // bool evaluateMessage(const std::string &user, AsyncClientCall *call)
   // {
   //    // std::cout << "Check write status:" << call->request.has_message() << std::endl;

   //    while (call->request.has_message() == 1 || !call->serverConnected)
   //    {
   //       // Wait until server is connected
   //       // Also wait until that client context request is processed successfully for future requests from that MAC
   //    }

   //    // if (user == "quit")
   //    // {
   //    //    std::cout << "**** Sending complete from: " << call << std::endl;
   //    //    call->stream_->WritesDone(reinterpret_cast<void *>(Type::WRITES_DONE));
   //    //    return false;
   //    // }

   //    // This is important: You can have at most one write or at most one read
   //    // at any given time. The throttling is performed by gRPC completion
   //    // queue. If you queue more than one write/read, the stream will crash.
   //    // Because this stream is bidirectional, you *can* have a single read
   //    // and a single write request queued for the same stream. Writes and reads
   //    // are independent of each other in terms of ordering/delivery.

   //    // Data we are sending to the server.
   //    if (call->request.has_message() == 0)
   //    {
   //       call->request.set_message(user);
   //       std::cout << "**** Sending request from: " << call << std::endl;
   //       call->stream_->Write(call->request, (void *)call);
   //    }
   //    return true;
   // }

   // uses the GRPC alarm to put tasks to the back of the completion queue
   void PutTaskBackToQueue(AsyncClientCall *call)
   {
      call->alarm_.reset(new Alarm);
      call->alarm_->Set(&cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), (void *)call);
   }

   // Out of the passed in Channel comes the stub, stored here, our view of the
   // server's exposed services.
   std::unique_ptr<GrpcHandler::Stub> stub_;

   // The producer-consumer queue we use to communicate asynchronously with the
   // gRPC runtime.
   CompletionQueue cq_;
};

const char *ParseCmdPara(char *argv, const char *para)
{
   auto p_target = std::strstr(argv, para);
   if (p_target == nullptr)
   {
      printf("para error argv[%s] should be %s \n", argv, para);
      return nullptr;
   }
   p_target += std::strlen(para);
   return p_target;
}

int main(int argc, char **argv)
{

   if (argc != 5)
   {
      std::cout << "Usage:./program --streamCount=xx --messageCount=xx --messageInterval=xx --port=xx";
      return 0;
   }

   int streamCount = std::atoi(ParseCmdPara(argv[1], "--streamCount="));
   int g_message_num = std::atoi(ParseCmdPara(argv[2], "--messageCount="));
   int messageInterval = std::atoi(ParseCmdPara(argv[3], "--messageInterval="));
   int g_port = std::atoi(ParseCmdPara(argv[4], "--port="));

   // Instantiate the client. It requires a channel, out of which the actual RPCs
   // are created. This channel models a connection to an endpoint (in this case,
   // localhost at port 50055). We indicate that the channel isn't authenticated
   // (use of InsecureChannelCredentials()).
   std::shared_ptr<grpc::Channel> channelName = grpc::CreateChannel(
       "localhost:"+std::to_string(g_port), grpc::InsecureChannelCredentials());
   GrpcHandlerClient greeter(channelName,
       streamCount);

   if (!greeter.serverConnected) {
      std::cout << "Server not connected by deadline of 24 hours" << std::endl;
      return 1;
   }

   // Spawn reader thread that loops indefinitely
   std::thread thread_ = std::thread(&GrpcHandlerClient::AsyncCompleteRpc, &greeter, channelName);

   int id = 0;
   time_t now = time(0);
   std::string macString = "00a0bc";

   // round robin messages between streams
   for (int i = 0; i < g_message_num; i++)
   {
      greeter.sendToQueue(macString, i, greeter.getNextContext());
      if (messageInterval) {
         // simulate time between messages
         sleep(messageInterval);
      }
   }

   std::cout << "Queue processing complete" << std::endl;

   for (int i = 0; i < greeter.contextQueue.size(); i++)
   {
      std::cout << "Pending size of contextID is " << greeter.getNextContext()->localQueue.size() << std::endl;
   }
   // Join the threads with the main thread
   // for (int i = 0; i < streamCount; ++i)
   // {
   //    std::cout << "Message processing for " << greeter.hashMap[id] << " complete" << std::endl;
   //    t[i].join();
   // }

   time_t then = time(0);
   std::cout << "Time difference for processing is all messages is " << then - now << " seconds" << std::endl;
   std::cout << "HO PREP's received: " << hoPrepCount << std::endl;

   // Below is the block for AsyncComplete queue which blocks for ever until we have a client restart..
   std::cout << "Press control-c to quit" << std::endl
               << std::endl;
   thread_.join(); // blocks forever

   // Verify client context is closed at the end of the run and there no bidirectional streams anymore
   while (!greeter.contextQueue.empty())
   {
      greeter.contextQueue.front()->stream_->Finish(&greeter.contextQueue.front()->finish_status_, (void *)greeter.contextQueue.front());
      std::cout << "Client " << greeter.contextQueue.front() << " no more after calling finish" << std::endl;
   }

   std::cout << "All bidirectional streams closed successfully" << std::endl;

   return 0;
}
