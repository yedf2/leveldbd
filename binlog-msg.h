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

void addBinlogHeader(Slice bkey, Slice ekey, HttpRequest& req, HttpResponse& resp);
void handleBinlog(LogDb* db, EventBase* base, const HttpConnPtr& con);
void sendEmptyBinlog(EventBase* base, LogDb* db);
void sendSyncReq(LogDb* db, EventBase* base, const HttpConnPtr& con);
void processSyncResp(LogDb* db, const HttpConnPtr& con, EventBase* base);

