#include "cocdb_client.h"

int main(int argc, char** argv) {
  grpc_init();

  CocDbClient cdb_client(grpc::CreateChannel("192.168.2.102:10000", grpc::InsecureCredentials(), ChannelArguments()));

  cdb_client.Shutdown();

  grpc_shutdown();
}
