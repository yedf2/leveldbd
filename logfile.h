#include <file.h>
#include <slice.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;
using namespace handy;

const int64_t LOG_MAGIC = 0x2323232323232323;

//record format
// magic len data padded
// 8      8  len  padded-to-8
struct LogFile {
    LogFile(): fd_(-1) {}
    Status open(const string& name, bool readonly=true);
    Status append(Slice record);
    Status getRecord(int64_t* offset, Slice* data, string* scrach);
    Status batchRecord(int64_t offset, string* rec, int batchSize);
    Status sync();
    int64_t size() { return lseek(fd_, 0, SEEK_END);}
    static Status decodeBinlogData(Slice* fileCont, Slice* record);

    int fd_;
    string name_;
    static size_t totalLen(size_t sz) { return (sz + 8 + 8+ 7) / 8 * 8; }
};

