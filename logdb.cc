#include "logdb.h"
#include <file.h>
#include "handler.h"
#include "binlog-msg.h"

int64_t FileName::binlogNum(const string& name) {
    Slice s1(name);
    if (s1.starts_with(binlogPrefix())) {
        Slice p1 = s1.sub(binlogPrefix().size());
        return util::atoi(p1.begin());
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
        s = Status::fromFormat(EINVAL, "dbid should be set a positive interger when binlog enabled");
        error("%s", s.toString().c_str());
    }
    if (s.ok()) {
        s = loadLogs_();
    }
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
        slaveStatus_.host = lns[c].eatWord();
    }
    if (lns.size() > ++c) {
        slaveStatus_.port = atoi(lns[c].data());
    }
    if (lns.size() > ++c) {
        slaveStatus_.key = lns[c].eatWord();
    }
    if (lns.size() > ++c) {
        slaveStatus_.fileno = util::atoi(lns[c].data());
    }
    if (lns.size() > ++c) {
        slaveStatus_.offset = util::atoi(lns[c].data());
        return Status();
    }
    st = Status::fromFormat(EINVAL, "bad format for slave status");
    error("%s", st.toString().c_str());
    return st;
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
        string lastfile = binlogDir_+FileName::binlogFile(logs.back());
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
            if (data.size() == 0) {
                error("unexpected end of logfile offset %ld sz %ld dsz %ld ignored",
                    offset, fsz, data.size());
                break;
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
                    s = Status::fromFormat(EINVAL, "unknown binlogOp %d", lr.op);
                    error("%s", s.toString().c_str());
                }
                break;
            }
        }
    }
    if (s.ok()) {
        s = file::writeContent(cfile, "0");
    }
    checkCurLog_();
    return s;
}

LogDb::~LogDb() {
    if (slaveStatus_.changed) {
        saveSlave_();
    }
    delete curLog_;
    if (binlogDir_.size()) {
        file::writeContent(binlogDir_ + FileName::closedFile(), "1");
    }
    delete db_;
}

Status LogDb::checkCurLog_() {
    Status st;
    if (curLog_ && curLog_->size() > binlogSize_) {
        st = curLog_->sync();
        if (!st.ok()) {
            return st;
        }
        delete curLog_;
        curLog_ = NULL;
    }
    if (curLog_ == NULL) {
        curLog_ = new LogFile();
        st = curLog_->open(binlogDir_+FileName::binlogFile(lastFile_+1), false);
        if (st.ok()) {
            lastFile_ ++;
        }
    }
    return st;
}

Status LogDb::write(Slice key, Slice value) {
    debug("write %.*s value len %ld", (int)key.size(), key.data(), value.size());
    LogRecord rec(dbid_, time(NULL), key, value, BinlogWrite);
    return applyRecord_(rec);
}

Status LogDb::remove(Slice key) {
    debug("remove %.*s", (int)key.size(), key.data());
    LogRecord rec(dbid_, time(NULL), key, "", BinlogDelete);
    return applyRecord_(rec);
}

Status LogDb::applyLog(Slice record) {
    LogRecord rec;
    Status st = LogRecord::decodeRecord(record, &rec);
    debug("applying %d %ld %s %.*s %d",
        rec.dbid, rec.tm, strOp(rec.op), (int)rec.key.size(), rec.key.data(), (int)rec.value.size());
    if (!st.ok() || rec.dbid == dbid_) { //ignore if dbid is self
        return st;
    }
    if (binlogDir_.size()) {
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
    Status st = Status::fromFormat(EINVAL, "unknown op in LogRecord %d", rec.op);
    error("%s", st.toString().c_str());
    return st;
}

Status LogDb::operateLog_(Slice data) {
    Status s = checkCurLog_();
    if (s.ok()) {
        s = curLog_->append(data);
    }
    vector<HttpConnPtr> conns = removeSlaveConnsLock();
    for (auto& con: conns) {
        EventBase* base = con->getBase();
        if (base) {
            handleBinlog(this, base, con);
        } else {
            error("connection closed, but sending response in operateLog");
        }
    }
    return s;
}

Status LogDb::saveSlave_() {
    string cont = util::format("%s #host\n%d #port\n%s # / begin key = end key\n%ld #binlog no\n%ld #binlog offset\n",
        slaveStatus_.host.c_str(), slaveStatus_.port, slaveStatus_.key.c_str(), slaveStatus_.fileno, slaveStatus_.offset);
    string fname = binlogDir_ + FileName::slaveFile();
    Status st = file::renameSave(fname, fname+".tmp", cont);
    if (!st.ok()) {
        error("save slave status failed %s", st.toString().c_str());
        return st;
    }
    info("save slave staus ok %s %ld %ld", slaveStatus_.key.c_str(), slaveStatus_.fileno, slaveStatus_.offset);
    slaveStatus_.changed = false;
    slaveStatus_.lastSaved = time(NULL);
    return Status();
}

Status LogDb::fetchLogLock(int64_t* fileno, int64_t* offset, string* data, const HttpConnPtr& con) {
    lock_guard<mutex> lk(*this);
    if (*fileno == lastFile_ && *offset == curLog_->size()) {
        slaveConns_.push_back(con);
        return Status();
    }
    if (*fileno > lastFile_ || (*fileno == lastFile_ && *offset > curLog_->size())) {
        error("qfile %ld qoff %ld larger than lastfile %ld off %ld while curlog==NULL",
            *fileno, *offset, lastFile_, curLog_->size());
        return Status::fromFormat(EINVAL, "file offset not valid");
    }
    Status st = getLog_(*fileno, *offset, data);
    if (!st.ok()) { //error
        error("db get log failed");
        return st;
    }
    if (data->empty()) {
        ++*fileno;
        *offset = 0;
    } else {
        *offset += data->size();
    }
    return Status();
}

Status LogDb::getLog_(int64_t fileno, int64_t offset, string* rec) {
    LogFile nf;
    LogFile* lf = NULL;
    Status st;
    if (curLog_ && lastFile_ == fileno) {
        lf = curLog_;
    } else {
        lf = &nf;
        st = nf.open(binlogDir_+FileName::binlogFile(fileno));
    }
    if (st.ok()) {
        st = lf->batchRecord(offset, rec, g_batch_size);
    }
    return st;
}

Status LogDb::updateSlaveStatusLock(Slice key, int64_t nfno, int64_t noff) {
    lock_guard<mutex> lk(*this);
    SlaveStatus& ss = slaveStatus_;
    if (nfno != ss.fileno || noff != ss.offset || ss.key != key) {
        ss.key = key;
        ss.fileno = nfno;
        ss.offset = noff;
        ss.changed = true;
        time_t now = time(NULL);
        if (now - ss.lastSaved > g_flush_slave_interval) {
            return saveSlave_();
        }
    }
    return Status();
}