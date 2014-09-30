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

struct SyncPos {
    string key;
    int64_t dataFinished;
    int64_t fileno;
    int64_t offset;
    SyncPos(): dataFinished(1), fileno(-1), offset(-1) {}
    string toString() { return util::format("%ld %ld %ld %s", fileno, offset, dataFinished, key.c_str()); }
    bool fromString(const string& pos, char delimiter) {
        return fromSlices(Slice(pos).split(delimiter));
    }
    bool fromSlices(const vector<Slice>& ss) {
        if (ss.size() < 3) {
            return false;
        }
        int i = 0;
        fileno = util::atoi(ss[i++].data());
        offset = util::atoi(ss[i++].data());
        dataFinished = util::atoi(ss[i++].data());
        if (ss.size() > 3) {
            Slice keyln = ss[i++];
            key = keyln.eatWord();
            if (key.size() && key[0] == '#') {
                key.clear();
            }
        }
        return true;
    }
    string toLines() { 
        return util::format("%ld #binlog file no\n%ld #binlog offset\n%ld #data file finished flag\n%s #current key\n",
            fileno, offset, dataFinished, key.c_str());
    }
    bool operator == (SyncPos& pos) { return key == pos.key && dataFinished == pos.dataFinished && fileno == pos.fileno && offset == pos.offset; }
    bool operator != (SyncPos& pos) { return !operator ==(pos); }
};
