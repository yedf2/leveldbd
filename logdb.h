#pragma once
#include <handy.h>
#include <http.h>
#include <logging.h>
#include <conf.h>
#include <status.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "globals.h"

struct LogFile: leveldb::WritableFile {
    LogFile():fd_(-1) {}
    Status open(const string& name);
    leveldb::Status Close();
    leveldb::Status Append(const leveldb::Slice& record);
    leveldb::Status Flush() { return leveldb::Status::OK(); }
    leveldb::Status Sync();
    size_t size() { return lseek(fd_, 0, SEEK_END); }
    ~LogFile() { Close(); }
    int fd_;
};

enum BinlogOp { BinlogWrite=1, BinlogDelete, };
struct LogRecord {
    int dbid;
    time_t tm;
    Slice key;
    Slice value;
    BinlogOp op;
    LogRecord():dbid(0),tm(0) {}
    LogRecord(int dbid1, time_t tm1, Slice key1, Slice value1, BinlogOp op1): dbid(dbid1),tm(tm1), key(key1), value(value1), op(op1) {}
    Status encodeRecord(string* data);
    static Status decodeRecord(Slice data, LogRecord* rec);
};

struct LogDb: public mutex {
    LogDb():dbid_(-1), binlogSize_(0), lastFile_(-1), curLog_(NULL), curLogFile_(NULL), db_(NULL) {}
    Status init(Conf& conf);
    leveldb::DB* getdb() { return db_; }
    Status write(Slice key, Slice value);
    Status remove(Slice key);
    Status applyLog(Slice record);
    ~LogDb();
    static Status dumpFile(const string& name);

    string binlogDir_, dbdir_;
    int dbid_;
    int binlogSize_;
    int lastFile_;
    void* curLog_;
    LogFile* curLogFile_;
    leveldb::DB* db_;

    Status checkCurLog_();
    Status applyRecord_(LogRecord& rec);
    Status operateDb_(LogRecord& rec);
    Status operateLog_(Slice data);
};
