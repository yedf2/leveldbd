#include "logdb.h"
#include <file.h>
#include <leveldb/env.h>
#include <db/log_reader.h>
#include <db/log_writer.h>

struct FileName {
    static string binlogPrefix() { return "binlog-"; }
    static bool isBinlog(const std::string& name) { return Slice(name).starts_with(binlogPrefix()); }
    static int64_t binlogNum(const string& name);
    static string binlogFile(int64_t no) { return binlogPrefix().data()+util::format("%05d", no); }
    static string closedFile() { return "/dbclosed.txt"; }
};

int64_t FileName::binlogNum(const string& name) {
    Slice s1(name);
    if (s1.starts_with(binlogPrefix())) {
        Slice p1 = s1.ltrim(binlogPrefix().size());
        char* pe = (char*)p1.end();
        return strtol(p1.begin(), &pe, 10);
    }
    return 0;
}

void bin_write(char*& p, const void* v, size_t len) {
    memcpy(p, v, len);
    p += len;
}
void bin_read(char*& p, void* v, size_t len) {
    memcpy(v, p, len);
    p += len;
}

template<class C> void bin_writeValue(char*& p, C v) {
    return bin_write(p, &v, sizeof(C));
}

template<class C> C bin_readValue(char*& p) {
    C c;
    bin_read(p, &c, sizeof(C));
    return c;
}

Status LogRecord::encodeRecord(string* data){
    data->clear();
    data->resize(4+8+4+4+key.size()+4+value.size());
    char* p = (char*)data->c_str();
    bin_writeValue(p, (int32_t)dbid);
    bin_writeValue(p, (int64_t)tm);
    bin_writeValue(p, (int32_t)op);
    bin_writeValue(p, (int32_t)key.size());
    bin_write(p, key.data(), key.size());
    bin_writeValue(p, (int32_t)value.size());
    bin_write(p, value.data(), value.size());
    assert(p == data->c_str() + data->size());
    return Status();
}

Status LogRecord::decodeRecord(Slice data, LogRecord* rec){
    Status es(EINVAL, "record length error");
    char* p = (char*)data.data();
    size_t isz = 4+8+4+4+4;
    if (data.size() < isz) {
        return es;
    }
    rec->dbid = bin_readValue<int32_t>(p);
    rec->tm = bin_readValue<int64_t>(p);
    rec->op = (BinlogOp)bin_readValue<int32_t>(p);
    size_t len = bin_readValue<int32_t>(p);
    if (data.size() < isz+len) {
        return es;
    }
    rec->key = Slice(p, len);
    p += len;
    size_t len2 = bin_readValue<int32_t>(p);
    if (data.size() != isz+len+len2) {
        return es;
    }
    rec->value = Slice(p, len2);
    p += len2;
    return Status();
}


Status LogFile::open(const string& name) {
    fd_ = ::open(name.c_str(), O_WRONLY|O_CREAT|O_APPEND, 0622);
    if (fd_ < 0) {
        Status s = Status::ioError("open", name);
        error("io error %s", s.toString().c_str());
        return s;
    }
    return Status();
}

leveldb::Status LogFile::Close() {
    if (fd_ >= 0) {
        int r = ::close(fd_);
        if (r < 0) {
            return leveldb::Status::IOError("close error");
        }
    }
    return leveldb::Status::OK();
}

leveldb::Status LogFile::Append(const leveldb::Slice& record) {
    int wd = ::write(fd_, record.data(), record.size());
    if (wd != (int)record.size()) {
        return leveldb::Status::IOError("write error");
    }
    return leveldb::Status::OK();
}

leveldb::Status LogFile::Sync() {
    int r = fsync(fd_);
    if (r < 0) {
        return leveldb::Status::IOError("fsync error");
    }
    return leveldb::Status::OK();
}

struct ErrorReporter: public leveldb::log::Reader::Reporter {
    virtual void Corruption(size_t bytes, const leveldb::Status& status) {
        error("corruption %lu bytes %s", bytes, status.ToString().c_str());
    }
};

Status LogDb::dumpFile(const string& name) {
    leveldb::SequentialFile* f = NULL;
    Status s = (ConvertStatus)leveldb::Env::Default()->NewSequentialFile(name, &f);
    if (s.ok()) {
        unique_ptr<leveldb::SequentialFile> rel1(f);
        ErrorReporter ep;
        leveldb::log::Reader reader(f, &ep, 1, 0);
        string scrach;
        leveldb::Slice rec;
        for (int i = 0; reader.ReadRecord(&rec, &scrach); i ++) {
            LogRecord lr;
            s = LogRecord::decodeRecord(convSlice(rec), &lr);
            if (!s.ok()) {
                break;
            }
            printf("record %d: op %s time %ld %s key %.*s value %.*s\n", i,
                lr.op==BinlogWrite?"WRITE":"DELETE", (long)lr.tm,
                util::readableTime(lr.tm).c_str(),
                (int)lr.key.size(), lr.key.data(),
                (int)lr.value.size(), lr.value.data());
        }
    }
    return s;
}

Status LogDb::init(Conf& conf) {

    dbdir_ = conf.get("", "dbdir", "ldb");
    dbdir_ = addSlash(dbdir_);
    leveldb::Options options;
    options.create_if_missing = true;
    Status s = (ConvertStatus)leveldb::DB::Open(options, dbdir_, &db_);
    fatalif(!s.ok(), "leveldb open failed %s", s.msg());

    binlogSize_ = conf.getInteger("", "binlog_size", 64*1024*1024);
    binlogDir_ = conf.get("", "binlog_dir", "");
    if (binlogDir_.empty()) {
        return s;
    }
    binlogDir_ = addSlash(binlogDir_);
    dbid_ = conf.getInteger("", "dbid", 0);
    if (dbid_ <= 0) {
        return Status::fromFormat(EINVAL, "dbid should be set a positive interger when binlog enabled");
    }
    file::createDir(binlogDir_); //ignore return value
    vector<string> files;
    s = file::getChildren(binlogDir_, &files);
    if (!s.ok()) return s;
    vector<int64_t> logs;
    for(size_t i = 0; i < files.size(); i ++) {
        int64_t n = FileName::binlogNum(files[i]);
        if (n) {
            logs.push_back(n);
        }
    }
    sort(logs.begin(), logs.end());
    if (logs.size()) { // remove last empty log file
        string lastfile = FileName::binlogFile(logs.back());
        uint64_t sz;
        Status s2 = file::getFileSize(lastfile, &sz);
        if (s2.ok() && sz == 0) {
            logs.pop_back();
        }
    }
    if (logs.size()) {
        lastFile_ = logs.back();
    }
    string cfile = binlogDir_ + FileName::closedFile().data();
    string cont;
    s = file::getContent(cfile, cont);
    if (s.ok() && cont != "1" && lastFile_) { //not elegantly closed, redo last log record
        string lastfile = binlogDir_ + FileName::binlogFile(lastFile_);
        leveldb::Env* env = leveldb::Env::Default();
        leveldb::SequentialFile* f = NULL;
        s = (ConvertStatus)env->NewSequentialFile(lastfile, &f);
        if (!s.ok()) {
            return s;
        }
        unique_ptr<leveldb::SequentialFile> rel1(f);
        ErrorReporter ep;
        leveldb::log::Reader reader(f, &ep, 1, 0);
        string scrach[2];
        leveldb::Slice rec[2];
        int i = 0;
        while(reader.ReadRecord(rec + (i%2), scrach+(i%2))) {
            i++;
        }
        if (i) {
            LogRecord lr;
            i = (i-1)%2;
            s = LogRecord::decodeRecord(convSlice(rec[i]), &lr);
            if (!s.ok()) {
                error("decode last binlog record failed %d %s", s.code(), s.msg());
            } else {
                if (lr.op == BinlogWrite) {
                    db_->Put(leveldb::WriteOptions(), convSlice(lr.key), convSlice(lr.value));
                } else if (lr.op == BinlogDelete) {
                    db_->Delete(leveldb::WriteOptions(), convSlice(lr.key));
                }
            }
        }
    }
    s = file::writeContent(cfile, "0");
    return s;
}

LogDb::~LogDb() {
    if (curLog_) {
        delete (leveldb::log::Writer*)curLog_;
    }
    if (curLogFile_) {
        delete curLogFile_;
    }
    if (binlogDir_.size()) {
        file::writeContent(binlogDir_ + FileName::closedFile(), "1");
    }
}

Status LogDb::checkCurLog_() {
    Status st;
    if (curLogFile_ && curLogFile_->size() > (size_t)binlogSize_) {
        delete (leveldb::log::Writer*)curLog_;
        curLog_ = NULL;
        st = (ConvertStatus)curLogFile_->Sync();
        delete curLogFile_;
        curLogFile_ = NULL;
    }
    if (!st.ok()) {
        return st;
    }
    if (curLog_ == NULL) {
        lastFile_++;
        curLogFile_ = new LogFile();
        st = curLogFile_->open(binlogDir_ + FileName::binlogFile(lastFile_));
        if (st.ok()) {
            curLog_ = new leveldb::log::Writer(curLogFile_);
        } else {
            delete curLogFile_;
            curLogFile_ = NULL;
        }
    }
    return st;

}

Status LogDb::write(Slice key, Slice value) {
    LogRecord rec(dbid_, time(NULL), key, value, BinlogWrite);
    return applyRecord_(rec);
}

Status LogDb::remove(Slice key) {
    LogRecord rec(dbid_, time(NULL), key, "", BinlogDelete);
    return applyRecord_(rec);
}

Status LogDb::applyLog(Slice record) {
    LogRecord rec;
    Status st = LogRecord::decodeRecord(record, &rec);
    if (!st.ok() || rec.dbid == dbid_) { //ignore if dbid is self
        return st;
    }
    if (binlogDir_.size()) {
        lock_guard<mutex> lk(*this);
        st = operateLog_(record);
        if (!st.ok()) {
            return st;
        }
        st = operateDb_(rec);
    } else {
        st = operateDb_(rec);
    }
    return st;
}

Status LogDb::applyRecord_(LogRecord& rec) {
    Status st;
    if (binlogDir_.size()) {
        lock_guard<mutex> lk(*this);
        string data;
        st = rec.encodeRecord(&data);
        if (!st.ok()) {
            return st;
        }
        st = operateLog_(data);
        if (!st.ok()) {
            return st;
        }
        st = operateDb_(rec);
    } else {
        st = operateDb_(rec);
    }
    return st;
}

Status LogDb::operateDb_(LogRecord& rec) {
    if (rec.op == BinlogWrite) {
        return (ConvertStatus)db_->Put(leveldb::WriteOptions(), convSlice(rec.key), convSlice(rec.value));
    } else if (rec.op == BinlogDelete) {
        return (ConvertStatus) db_->Delete(leveldb::WriteOptions(), convSlice(rec.key));
    }
    return Status::fromFormat(EINVAL, "unknown op in LogRecord %d", rec.op);
}

Status LogDb::operateLog_(Slice data) {
    Status s;
    s = checkCurLog_();
    if (s.ok()) {
        leveldb::log::Writer* wr = (leveldb::log::Writer*)curLog_;
        s = (ConvertStatus)wr->AddRecord(convSlice(data));
    }
    return s;
}

