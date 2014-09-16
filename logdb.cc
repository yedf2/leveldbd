#include "logdb.h"
#include <file.h>

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


Status LogDb::dumpFile(const string& name) {

    LogFile lf;
    Status st = lf.open(name);
    if (st.ok()) {
        Slice rec;
        int64_t offset = 0;
        string scrach;
        LogRecord lr;
        int i = 0;
        for (;;) {
            st = lf.getRecord(&offset, &rec, &scrach);
            if (!st.ok() || rec.size() == 0) {
                break;
            }
            st = LogRecord::decodeRecord(rec, &lr);
            if (!st.ok()) {
                break;
            }
            printf("record %d: op %s time %ld %s key %.*s value %.*s\n", ++i,
                lr.op==BinlogWrite?"WRITE":"DELETE", (long)lr.tm,
                util::readableTime(lr.tm).c_str(),
                (int)lr.key.size(), lr.key.data(),
                (int)lr.value.size(), lr.value.data());
        }
    }
    return st;
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
    s = loadLogs_();
    if (s.ok()) {
        s = loadSlave_();
    }
    return s;
}

Status LogDb::loadSlave_() {
    string cont;
    Status st = file::getContent(binlogDir_ + FileName::slaveFile(), cont);
    if (st.code() == ENOENT) {
        return Status();
    }
    Slice data = cont;
    vector<Slice> lns = data.split('\n');
    size_t c = 0;
    if (lns.size() > c) {
        slave_.host = lns[c].eatWord();
    }
    if (lns.size() > ++c) {
        slave_.port = atoi(lns[c].data());
    }
    if (lns.size() > ++c) {
        slave_.key = lns[c].eatWord();
    }
    if (lns.size() > ++c) {
        char* e = (char*)lns[c].end();
        slave_.fileno = strtol(lns[c].data(), &e, 10);
    }
    if (lns.size() > ++c) {
        char* e = (char*)lns[c].end();
        slave_.offset = strtol(lns[c].data(), &e, 10);
        return Status();
    }
    return Status::fromFormat(EINVAL, "bad format for slave status");
}

Status LogDb::loadLogs_() {
    file::createDir(binlogDir_); //ignore return value
    vector<string> files;
    Status s = file::getChildren(binlogDir_, &files);
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
    if (s.code() == ENOENT) { //ignore
        s = Status();
    } else if (s.ok() && cont != "1" && lastFile_) { //not elegantly closed, redo last log record
        string lastfile = binlogDir_ + FileName::binlogFile(lastFile_);
        size_t fsz = 0;
        s = file::getFileSize(lastfile, &fsz);
        if (!s.ok()) {
            return s;
        }
        LogFile lf;
        s = lf.open(lastfile);
        if (!s.ok()) {
            return s;
        }
        int64_t offset = 0;
        Slice data;
        string scrach;
        for (;;) {
            s = lf.getRecord(&offset, &data, &scrach);
            if (!s.ok()) {
                return s;
            }
            if (offset == (int64_t)fsz) {
                LogRecord lr;
                s = LogRecord::decodeRecord(data, &lr);
                if (!s.ok()) {
                    return s;
                }
                if (lr.op == BinlogWrite) {
                    s = (ConvertStatus)db_->Put(leveldb::WriteOptions(), convSlice(lr.key), convSlice(lr.value));
                } else if (lr.op == BinlogDelete) {
                    s = (ConvertStatus)db_->Delete(leveldb::WriteOptions(), convSlice(lr.key));
                } else {
                    return Status::fromFormat(EINVAL, "unknown binlogOp %d", lr.op);
                }

            }
        }
    }
    if (s.ok()) {
        s = file::writeContent(cfile, "0");
    }
    return s;
}

LogDb::~LogDb() {
    delete curLog_;
    if (binlogDir_.size()) {
        file::writeContent(binlogDir_ + FileName::closedFile(), "1");
    }
}

Status LogDb::checkCurLog_() {
    Status st;
    if (curLog_ && curLog_->size() > binlogSize_) {
        st = curLog_->sync();
        delete curLog_;
        curLog_ = NULL;
    }
    if (!st.ok()) {
        return st;
    }
    if (curLog_ == NULL) {
        curLog_ = new LogFile();
        st = curLog_->open(binlogDir_+FileName::binlogFile(lastFile_+1));
        if (!st.ok()) {
            delete curLog_;
            curLog_ = NULL;
        } else {
            lastFile_ ++;
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
    Status s = checkCurLog_();
    if (s.ok()) {
        s = curLog_->append(data);
    }
    return s;
}

