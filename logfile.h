#include <file.h>
#include <slice.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;
using namespace handy;

struct LogFile {
    LogFile(): fd_(-1) {}
    Status open(const string& name);
    Status append(Slice record);
    Status getRecord(int64_t* offset, Slice* data, string* scrach);
    Status sync() { int r = fsync(fd_); return Status::fromSystem(r); }
    int64_t size() { return lseek(fd_, 0, SEEK_END);}

    int fd_;
    size_t padLen(size_t sz) { return (sz + 8 + 8 + 7) / 8 * 8; }
};

