#include "binlog-msg.h"
#include "handler.h"

void handleBinlog(LogDb* db, EventBase* base, const HttpConnPtr& con) {
    HttpRequest& req = con->getRequest();
    string sf = req.getArg("f");
    string soff = req.getArg("off");
    if (sf.empty() || soff.empty()) {
        error("empty arg f or off");
        con->close();
        return;
    }
    SyncPos pos;
    pos.fileno = util::atoi(sf.c_str());
    pos.offset = util::atoi(soff.c_str());
    HttpResponse& resp = con->getResponse();
    resp.headers["req-info"] = pos.toString();
    SyncPos npos = pos;
    Status st = db->fetchLogLock(&npos.fileno, &npos.offset, &resp.body, con);
    if (!st.ok()) {
        con->getResponse().setStatus(500, st.toString());
        base->safeCall([con]{con->sendResponse(); });
        return;
    } else if (pos.fileno == npos.fileno && pos.offset == npos.offset) {
        return;
    }
    resp.headers["next-info"] = npos.toString();
    info("binlog response req-info '%s' next-info '%s' body len %ld", 
        resp.getHeader("req-info").c_str(), resp.getHeader("next-info").c_str(), resp.body.size());
    base->safeCall([con]{con->sendResponse(); });
}

void addBinlogHeader(Slice bkey, Slice ekey, HttpRequest& req, HttpResponse& resp) {
    string reqinfo = req.getHeader("req-info");
    if (reqinfo.size()) {
        resp.headers["req-info"] = reqinfo;
        SyncPos pos;
        bool r = pos.fromString(reqinfo, ' ');
        if (!r) {
            error("sync pos decode failed %s", reqinfo.c_str());
            return;
        }
        if (ekey.empty()) {
            pos.key.clear();
            pos.dataFinished = 1;
        } else {
            pos.key = ekey;
        }
        resp.headers["next-info"] = pos.toString();
    }
}

void sendEmptyBinlog(EventBase* base, LogDb* db) {
    vector<HttpConnPtr> conns = db->removeSlaveConnsLock();
    for (auto& con: conns) {
        HttpResponse& resp = con->getResponse();
        resp.headers["next-info"] = resp.headers["req-info"];
        info("binlog response %s empty resp", resp.getHeader("req-info").c_str());
        con->sendResponse();
    }
}

void sendSyncReq(LogDb* db, EventBase* base, const HttpConnPtr& con) {
    SlaveStatus ss = db->getSlaveStatusLock();
    HttpRequest& req = con->getRequest();
    req.headers["req-info"] = ss.pos.toString();
    if (!ss.pos.dataFinished) {
        req.query_uri = "/range-get/" + ss.pos.key;
    } else {
        req.query_uri = util::format("/binlog/?f=%05ld&off=%ld", ss.pos.fileno, ss.pos.offset);
    }
    debug("geting %s", req.query_uri.c_str());
    base->safeCall([con] { con->sendRequest();});
}

void processSyncResp(LogDb* db, const HttpConnPtr& con, EventBase* base) {
    bool isError = true;
    ExitCaller atend([&]{ if (isError) con->close(); else sendSyncReq(db, base, con); });

    HttpResponse& res = con->getResponse();
    if (res.status != 200) {
        error("response error. code %d", res.status);
        return;
    }
    Slice body = res.getBody();
    string reqinfo = res.getHeader("req-info");
    SyncPos pos;
    bool r = pos.fromString(reqinfo, ' ');
    if (!r) {
        error("unexpected header req-info '%s'", reqinfo.data());
        return;
    }
    SlaveStatus ss = db->getSlaveStatusLock();
    if (pos != ss.pos) {
        error("header req-info '%s' not match slave status '%s'",
            pos.toString().c_str(), ss.pos.toString().c_str());
        return;
    }

    Status st;
    if (pos.dataFinished == 0) { //range-get resp
        Slice key, value;
        bool exist;
        while (body.size() && (st=decodeKvBody(&body, &key, &value, &exist), st.ok())) {
            st = db->write(key, value);
            if (!st.ok()) {
                break;
            }
        }
    } else { //binlog resp
        Slice record;
        while (body.size() && (st=LogFile::decodeBinlogData(&body, &record), st.ok())) {
            st = db->applyLog(record);
            if (!st.ok()) {
                break;
            }
        }
    }
    if (st.ok()) {
        string nextinfo = res.getHeader("next-info");
        SyncPos pos;
        bool r = pos.fromString(nextinfo, ' ');
        if (!r) {
            error("unexpected header next-info %s", nextinfo.c_str());
            con->close();
            return;
        } else {
            st = db->updateSlaveStatusLock(pos);
        }
    }
    isError = false;
}


