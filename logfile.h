#include <file.h>
#include <slice.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;
using namespace handy;

const int64_t LOG_MAGIC = 0x2323232323232323;

struct LogFile {
    LogFile(): fd_(-1) {}
    Status open(const string& name);
    Status append(Slice record);
    Status getRecord(int64_t* offset, Slice* data, string* scrach);
    Status sync();
    int64_t size() { return lseek(fd_, 0, SEEK_END);}

    int fd_;
    string name_;
    size_t padLen(size_t sz) { return (sz + 8 + 8 + 7) / 8 * 8; }
};

