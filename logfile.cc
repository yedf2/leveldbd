#include "logfile.h"
#include <net.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <memory>
#include <logging.h>

Status LogFile::open(const string& name, bool readonly) {
    name_ = name;
    Status st;
    int flag = O_RDWR|O_APPEND;
    if (!readonly) {
        flag |= O_CREAT;
    }
    fd_ = ::open(name.c_str(), flag, 0622);
    if (fd_ < 0) {
        st = Status::ioError("open", name);
        error("%s", st.toString().c_str());
    }
    if (!readonly) {
        info("open logfile %s %s", name.c_str(), st.toString().c_str());
    }
    return Status();
}

Status LogFile::append(Slice record) {
    int padded = totalLen(record.size());
    char* p = new char[padded];
    memset(p, 0, padded);
    unique_ptr<char> rel1(p);
    memcpy(p, &LOG_MAGIC, 8);
    *(size_t*)(p+8) = record.size();
    memcpy(p+16, record.data(), record.size());
    int w = ::write(fd_, p, padded);
    if (w != padded) {
        Status st = Status::ioError("write", name_);
        error("%s", st.toString().c_str());
        return st;
    }
    return Status();
}

Status LogFile::getRecord(int64_t* offset, Slice* data, string* scrach) {
    int64_t head[2] = {0, 0} ;
    *data = Slice();
    int r = pread(fd_, head, 16, *offset);
    if (r == 0) {
        return Status();
    }
    int64_t magic = head[0], len = head[1];
    int r2 = -1;
    if (r == 16 && magic == LOG_MAGIC) {
        int padded = totalLen(len);
        scrach->resize(padded-16);
        char* p = (char*)scrach->c_str();
        r2 = pread(fd_, p, padded-16, *offset+16);
        if (r2 == padded-16) {
            *data = Slice(p, len);
            *offset += padded;
            return Status();
        }
    }
    Status st = Status::fromFormat(EINVAL, "getrecord error r %d r2 %d len %ld magic %lx off %ld errno %d %s",
        r, r2, len, magic, *offset, errno, errstr());
    error("%s", st.toString().c_str());
    return st;
}

Status LogFile::sync() { 
    int r = fsync(fd_); 
    if (r<0) { 
        Status st = Status::ioError("fsync", name_);
        error("%s", st.toString().c_str());
        return st; 
    }
    return Status();
}

Status LogFile::batchRecord(int64_t offset, string* rec, int batchSize) {
    char* p = new char[batchSize];
    unique_ptr<char[]> rel1(p);
    int r = pread(fd_, p, batchSize, offset);
    Status st;
    if (r < 0) {
        st = Status::ioError("pread", name_);
        error("logfile batchRecord %s", st.toString().c_str());
        return st;
    }
    if (r == 0) {
        return Status();
    }
    char* pe = p + r;
    char* pb = p;
    int64_t magic = 0;
    int64_t len = 0;
    while (pb + 16 <= pe) {
        magic = *(int64_t*)pb;
        len = *(int64_t*)(pb+8);
        if (magic != LOG_MAGIC || len < 0) {
            error("logfile bad format magic %lx len %ld at %s %ld",
                magic, len, name_.c_str(), offset+pb-p);
            return Status::fromFormat(EINVAL, "bad format log file %s", name_.c_str());
        }
        int64_t tlen = totalLen(len);
        if (pb + tlen > pe) {
            break;
        }
        pb += tlen;
    }
    if (pb == p) {
        error("log record invalid. readed %ld len %ld batch_size %d", pe-p, len, batchSize);
        return Status::fromFormat(EINVAL, "bad format");
    }
    rec->clear();
    rec->append(p, pb);
    return Status();
}

Status LogFile::decodeBinlogData(Slice* fileCont, Slice* record) {
    Status inval = Status::fromFormat(EINVAL, "bad format for binlog resp");
    if (fileCont->empty()) {
        error("empty fileCont");
        return inval;
    }
    int64_t magic = *(int64_t*)fileCont->begin();
    if (magic != LOG_MAGIC) {
        error("bad magic no in binlog data");
        return inval;
    }
    int64_t len = *(int64_t*)(fileCont->begin()+8);
    int64_t tlen = totalLen(len);
   
    if (fileCont->begin()+tlen > fileCont->end()) {
        error("bad length in binlog resp tlen %ld body %ld", tlen, fileCont->size());
        return inval;
    }
    *record = Slice(fileCont->begin()+16, len);
    *fileCont = Slice(fileCont->begin()+tlen, fileCont->end());
    return Status();
}

