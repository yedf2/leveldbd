#pragma once
#include <handy/handy.h>
#include <handy/http.h>
#include <handy/conf.h>
#include <handy/status.h>
#include "leveldb/db.h"

using namespace std;
using namespace handy;

extern Conf g_conf;
extern int g_page_limit;
extern int g_batch_count;
extern int g_batch_size;
extern int g_flush_slave_interval;

void setGlobalConfig(Conf& conf);
inline leveldb::Slice convSlice(Slice s) { return leveldb::Slice(s.data(), s.size()); }
inline Slice convSlice(leveldb::Slice s) { return Slice(s.data(), s.size()); }
inline string addSlash(const string& dir) { if (dir.size() && dir[dir.size()-1] != '/') return dir + '/'; return dir; }

struct ConvertStatus {
    Status st;
    ConvertStatus(const leveldb::Status& s){ if (!s.ok()) { st = Status(EINVAL, s.ToString()); } }
    operator Status () { return move(st); }
};


