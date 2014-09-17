#include "logfile.h"
#include <net.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <memory>
#include <logging.h>

Status LogFile::open(const string& name) {
    name_ = name;
    fd_ = ::open(name.c_str(), O_RDWR|O_CREAT|O_APPEND, 0622);
    if (fd_ < 0) {
        Status st = Status::ioError("open", name);
        error("%s", st.toString().c_str());
    }
    return Status();
}

Status LogFile::append(Slice record) {
    int padded = padLen(record.size());
    char* p = new char[padded];
    unique_ptr<char> rel1(p);
    *(size_t*)p = net::hton(record.size());
    memcpy(p+8, &LOG_MAGIC, 8);
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
    int r2 = -1;
    if (r == 16 && head[1] == LOG_MAGIC) {
        int padded = padLen(net::ntoh(head[0]));
        scrach->resize(padded-16);
        char* p = (char*)scrach->c_str();
        r2 = pread(fd_, p, padded-16, *offset+16);
        if (r2 == padded-16) {
            *data = Slice(p, head[0]);
            *offset += padded;
            return Status();
        }
    }
    Status st = Status::fromFormat(EINVAL, "getrecord error r %d r2 %d len %ld magic %lx off %ld errno %d %s",
        r, r2, head[0], head[1], *offset, errno, errstr());
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
