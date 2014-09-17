#pragma once
#include <handy.h>
#include <http.h>
#include <logging.h>
#include <conf.h>
#include "leveldb/db.h"
#include "globals.h"
#include "logdb.h"

using namespace std;
using namespace handy;

int64_t getSize(Slice bkey, Slice ekey, leveldb::DB* db);

void handleReq(EventBase& base, LogDb* db, const HttpConnPtr& con);

