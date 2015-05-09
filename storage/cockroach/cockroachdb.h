#ifndef COCKROACHDB_H_
#define COCKROACHDB_H_

#include <iostream>
#include "slice.h"

#include <grpc/grpc.h>
#include <grpc++/channel_arguments.h>
#include <grpc++/channel_interface.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/credentials.h>
#include <grpc++/status.h>
#include "cocdb.grpc.pb.h"

using grpc::ChannelArguments;
using grpc::ChannelInterface;
using grpc::ClientContext;
using grpc::Status;
using cdb::CDB;
using cdb::GetRequest;
using cdb::GetResponse;
using cdb::PutRequest;
using cdb::PutResponse;
using cdb::Operation;
using cdb::BeginRequest;
using cdb::CommitResponse;
using cdb::IterRequest;
using cdb::IterResponse;
using cdb::Trx;

class CockroachDB {
 public:
  CockroachDB(std::shared_ptr<ChannelInterface> channel)
    : stub_(CDB::NewStub(channel)) {}

  bool PutAndCommit(const cockroachdb::Slice& key, const cockroachdb::Slice& val) {
    std::string trx_id;
    if (Begin(&trx_id)) {
      std::cout << "begin put trx " << trx_id << std::endl;
      if (Put(trx_id, key, val)) {
        bool ok = Commit(trx_id);
        std::cout << "put " << key.ToString(true) << " " << val.ToString(true) << " " << ok << std::endl;
        return ok;
      }
    }
    return false;
  }

  bool DeleteAndCommit(const cockroachdb::Slice& key) {
    std::string trx_id;
    if (Begin(&trx_id)) {
      if (Delete(key)) {
        return Commit(trx_id);
      }
    }
    return false;
  }

  bool Seek(const std::string& trx_id, const std::string& offset_key, std::string* cur_val, std::string* cur_key) {
    IterRequest req;
    req.set_offset_key(offset_key);
    req.set_trx_id(trx_id);
    req.set_direction(IterRequest::Next);
    req.set_cnt(1);

    IterResponse reply;
    ClientContext ctx;

    Status status = stub_->Iter(&ctx, req, &reply);
    if (status.IsOk()) {
      *cur_key = reply.cur_key();
    } else {
      return false;
    }

    std::string ret;
    if (Get(trx_id, reply.cur_key(), &ret)) {
      *cur_val = ret;
      return true;
    }
    return false;
  }

  bool Put(const std::string& trx_id, const cockroachdb::Slice& key, const cockroachdb::Slice& value) {
    PutRequest req;
    Operation* op = req.add_ops();
    op->set_type(Operation::PUT);
    op->set_key(key.ToString());
    op->set_val(value.ToString());

    req.set_trx_id(trx_id);
    PutResponse reply;
    ClientContext context;

    Status status = stub_->Put(&context, req, &reply);
    if (status.IsOk()) {
      return true;
    }
    return false;
  }

  bool Get(const std::string& trx_id, const cockroachdb::Slice& key, std::string* val) {
    GetRequest request;
    request.set_key(key.ToString());
    request.set_trx_id(trx_id);

    GetResponse reply;
    ClientContext context;

    Status status = stub_->Get(&context, request, &reply);
    if (status.IsOk()) {
      *val = reply.val();
      return true;
    }
    return false;
  }

  bool Delete(const cockroachdb::Slice& key) {
    return true;
  }

  bool Begin(std::string* trx_id) {
    BeginRequest req;
    Trx reply;
    ClientContext ctx;

    Status status = stub_->BeginTrx(&ctx, req, &reply);
    if (status.IsOk()) {
      *trx_id = reply.trx_id();
      return true;
    }
    return false;
  }

  bool Commit(const std::string& trx_id) {
    Trx trx;
    CommitResponse reply;
    trx.set_trx_id(trx_id);
    ClientContext ctx;

    Status status = stub_->CommitTrx(&ctx, trx, &reply);
    if (status.IsOk()) {
      return true;
    }
    return false;
  }

 private:
  std::unique_ptr<CDB::Stub> stub_;
};

class CockroachIterator {
 public:
  CockroachIterator(CockroachDB* db, const std::string& trx_id):
    trx_id_(trx_id),
    valid_(false),
    cdb_(db) {
  }

  void Seek(const cockroachdb::Slice& offset) {
    std::string cur_key;
    std::string cur_val;
    std::string trx;

    if (cdb_->Seek(trx_id_, offset.ToString(), &cur_val, &cur_key) && !cur_key.empty()) {
      cur_key_ = cur_key;
      cur_val_ = cur_val;
      valid_ = true;
    } else {
      valid_ = false;
    }
  }

  bool Valid() {
    return valid_;
  }

  void Next() {
    std::string key;
    std::string val;
    std::string next_offset_key = cur_key_ + '\0';
    if (cdb_->Seek(trx_id_, next_offset_key, &val, &key) && !key.empty()) {
      cur_key_ = key;
      cur_val_ = val;
      valid_ = true;
    } else {
      valid_ = false;
    }
  }

  cockroachdb::Slice key() {
    return cockroachdb::Slice(cur_key_.c_str(), cur_key_.size());
  }

  cockroachdb::Slice value() {
    return cockroachdb::Slice(cur_val_.c_str(), cur_val_.size());
  }

 private:
  std::string cur_key_;
  std::string cur_val_;
  std::string trx_id_;

  bool valid_;
  CockroachDB* cdb_;
};

#endif
