#pragma once
#include <handy/handy.h>
#include <handy/http.h>
#include <handy/conf.h>
#include <handy/status.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "globals.h"
#include "logfile.h"

struct FileName {
    static string binlogPrefix() { return "binlog-"; }
    static bool isBinlog(const std::string& name) { return Slice(name).starts_with(binlogPrefix()); }
    static int64_t binlogNum(const string& name);
    static string binlogFile(int64_t no) { return binlogPrefix().data()+util::format("%05d", no); }
    static string closedFile() { return "dbclosed.txt"; }
    static string slaveFile() { return "slave-status"; }
};

enum BinlogOp { BinlogWrite=1, BinlogDelete, };

inline const char* strOp(BinlogOp op) {
    if (op == BinlogDelete) {
        return "Delete";
    } else if (op == BinlogWrite) {
        return "Write";
    }
    return "Unkown";
};

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

struct SlaveStatus {
    string host;
    int port;
    SyncPos pos;
    time_t lastSaved;
    bool changed;
    SlaveStatus():port(-1), lastSaved(time(NULL)), changed(0) {}
    bool isValid() { return pos.offset != -1; }
};

struct LogDb: public mutex {
    LogDb():dbid_(-1), binlogSize_(0), lastFile_(0), curLog_(NULL), db_(NULL) {  }
    Status init(Conf& conf);
    leveldb::DB* getdb() { return db_; }
    Status write(Slice key, Slice value);
    Status remove(Slice key);
    Status applyLog(Slice record);
    ~LogDb();
    vector<HttpConnPtr> removeSlaveConnsLock() { lock_guard<mutex> lk(*this); return move(slaveConns_); }
    SlaveStatus getSlaveStatusLock() { lock_guard<mutex> lk(*this); return slaveStatus_; }
    Status updateSlaveStatusLock(SyncPos pos);
    Status fetchLogLock(int64_t* fileno, int64_t* offset, string* data, const HttpConnPtr& con);
    static Status dumpFile(const string& name);


    SlaveStatus slaveStatus_;
    string binlogDir_, dbdir_;
    int dbid_;
    int binlogSize_;
    int64_t lastFile_;
    LogFile* curLog_;
    leveldb::DB* db_;
    vector<HttpConnPtr> slaveConns_;

    Status getLog_(int64_t fileno, int64_t offset, string* rec);
    Status saveSlave_();
    Status checkCurLog_();
    Status applyRecord_(LogRecord& rec);
    Status operateDb_(LogRecord& rec);
    Status operateLog_(Slice data);
    Status loadLogs_();
    Status loadSlave_();
};
