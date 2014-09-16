#include "logfile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <memory>

Status LogFile::open(const string& name) {
    fd_ = ::open(name.c_str(), O_RDWR|O_CREAT|O_APPEND, 0622);
    if (fd_ < 0) {
        return Status::ioError("open", name);
    }
    return Status();
}

Status LogFile::append(Slice record) {
    int padded = padLen(record.size());
    char* p = new char[padded];
    unique_ptr<char> rel1(p);
    *(size_t*)p = record.size();
    memcpy(p+8, "########", 8);
    memcpy(p+16, record.data(), record.size());
    int w = ::write(fd_, p, padded);
    if (w != padded) {
        return Status::fromSystem();
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
    if (r == 16 && head[1] == 0x2323232323232323) {
        int padded = padLen(head[0]);
        scrach->resize(padded-16);
        char* p = (char*)scrach->c_str();
        r2 = pread(fd_, p, padded-16, *offset+16);
        if (r2 == padded-16) {
            *data = Slice(p, head[0]);
            *offset += padded;
            return Status();
        }
    }
    return Status::fromFormat(EINVAL, "getrecord error r %d r2 %d len %ld magic %lx off %ld errno %d %s",
        r, r2, head[0], head[1], *offset, errno, errstr());
}

