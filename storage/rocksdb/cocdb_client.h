#ifndef COCDB_CLIENT_H
#define COCDB_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/write_batch.h"

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

class CocDbIterator;

class CocDbClient {
 public:
  CocDbClient(std::shared_ptr<ChannelInterface> channel)
      : stub_(CDB::NewStub(channel)) {}

  bool doSeek(const std::string& trx_id, const std::string& key, std::string* val, std::string* next_key) {
    IterRequest req;
    req.set_offset_key(key);
    req.set_trx_id(trx_id);
    req.set_direction(IterRequest::Next);
    req.set_cnt(1);

    IterResponse reply;
    ClientContext ctx;

    Status status = stub_->Iter(&ctx, req, &reply);
    if (status.IsOk()) {
      *next_key = reply.cur_key();
    } else {
      return false;
    }

    std::string ret;
    if (Get(reply.cur_key(), &ret)) {
      *val = ret;
      return true;
    }

    return false;
  }


  bool Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    std::string trx_id;
    if (this->Begin(&trx_id)) {
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
        this->Commit(trx_id);
        return true;
      }
    }
    return false;
  }

  void Delete(const rocksdb::Slice& key) {
  }

  bool Get(const rocksdb::Slice& key, std::string* val) {
    GetRequest request;
    request.set_key(key.ToString());
    GetResponse reply;
    ClientContext context;

    Status status = stub_->Get(&context, request, &reply);
    if (status.IsOk()) {
      *val = reply.val();
      return true;
    }
    return false;
  }

  class BatchIterateHandler : public rocksdb::WriteBatch::Handler {
   public:
    rocksdb::Slice value_;
    rocksdb::Slice key_;
    virtual void Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        key_ = key;
        value_ = value;
    }

    virtual void Delete(const rocksdb::Slice& key) {
    }
  };

  bool Write(rocksdb::WriteBatch* batch) {
    BatchIterateHandler handler;
    batch->Iterate(&handler);
    return Put(handler.key_, handler.value_);
  }

  void Shutdown() { stub_.reset(); }

  bool Begin(std::string* trx_id) {
    BeginRequest req;
    Trx reply;
    ClientContext context;

    Status status = stub_->BeginTrx(&context, req, &reply);
    if (status.IsOk()) {
      std::cout << reply.trx_id() << std::endl;
      *trx_id = reply.trx_id();
      return true;
    }
    return false;
  }

  bool Commit(const std::string& trx_id) {
    Trx trx;
    CommitResponse reply;

    trx.set_trx_id(trx_id);
    ClientContext context;
    Status status = stub_->CommitTrx(&context, trx, &reply);
    if (status.IsOk()) {
      return true;
    }
    return false;
  }

 private:
  std::unique_ptr<CDB::Stub> stub_;
};

class CocDbIterator {
 public:
    CocDbIterator(CocDbClient* cocdb, const std::string& trx_id):
      cocdb_(cocdb),
      trx_id_(trx_id)
    {}

    void Seek(const rocksdb::Slice& entry) {
      std::string next;
      std::string val;
      if (trx_id_.empty()) {
        std::string trx;
        if (cocdb_->Begin(&trx)) {
            trx_id_ = trx;
            std::cout << "trx id:" << trx_id_ << std::endl;
            std::cout << "do seek" << std::endl;
            if (cocdb_->doSeek(trx_id_, entry.ToString(), &val, &next)) {
              next_key_ = next;
              cur_val_ = val;
              cur_key_ = entry.ToString();
              valid_ = true;
              std::cout <<"cur val:" << cur_val_ << " " << "cur key: " << cur_key_ << " next key: " << next_key_ << std::endl;
            } else {
              std::cout << "seek occur error" << std::endl;
              valid_ = false;
            }
            std::cout << "end do seek" << std::endl;
            cocdb_->Commit(trx_id_);
            std::cout << "bye" << std::endl;
        }
      } else {
        if (cocdb_->doSeek(trx_id_, entry.ToString(), &val, &next)) {
          next_key_ = next;
          cur_key_ = entry.ToString();
          cur_val_ = val;
          valid_ = true;
          std::cout <<"cur val:" << cur_val_ << " " << "cur key: " << cur_key_ << " next key: " << next_key_ << std::endl;
        } else {
          valid_ = false;
        }
      }
    }

    void SeekToLast() {}

    bool Valid() {
      return valid_;
    }

    void Next() {
      std::string next;
      std::string val;
      if (trx_id_.empty()) {
        std::string trx;
        if (cocdb_->Begin(&trx)) {
            trx_id_ = trx;
            if (cocdb_->doSeek(trx_id_, next_key_, &val, &next)) {
                cur_key_ = next_key_;
                cur_val_ = val;
                next_key_ = next;
                valid_ = true;
                std::cout <<"cur val:" << cur_val_ << " " << "cur key: " << cur_key_ << " next key: " << next_key_ << std::endl;
            } else {
                valid_ = false;
            }
            cocdb_->Commit(trx_id_);
        }
      } else {
        if (cocdb_->doSeek(trx_id_, next_key_, &val, &next)) {
          cur_key_ = next_key_;
          cur_val_ = val;
          next_key_ = next;
          valid_ = true;
        } else {
          valid_ = false;
        }
      }
    }

    void Prev() {}
    rocksdb::Slice key() { return rocksdb::Slice(cur_key_.c_str(), cur_key_.size()); }
    rocksdb::Slice value() { return rocksdb::Slice(cur_val_.c_str(), cur_val_.size()); }
    bool ok() { return true;}
 private:
    CocDbClient* cocdb_;
    std::string trx_id_;
    std::string next_key_;
    std::string cur_key_;
    std::string cur_val_;
    bool valid_;
};

#endif
