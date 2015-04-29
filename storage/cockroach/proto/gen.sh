#!/bin/sh
protoc -I . --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` cocdb.proto
protoc -I . --cpp_out=. cocdb.proto
