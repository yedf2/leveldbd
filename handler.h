#pragma once
#include <handy.h>
#include <http.h>
#include <logging.h>
#include <conf.h>
#include "leveldb/db.h"

using namespace std;
using namespace handy;

extern Conf g_conf;
extern int g_page_limit;
extern int g_batch_count;
extern int g_batch_size;

void setGlobalConfig(Conf& conf);

void handleReq(EventBase& base, leveldb::DB* db, const TcpConnPtr& con);
inline leveldb::Slice convSlice(Slice s) { return leveldb::Slice(s.data(), s.size()); }
inline Slice convSlice(leveldb::Slice s) { return Slice(s.data(), s.size()); }

