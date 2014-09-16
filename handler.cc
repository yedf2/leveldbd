#include "handler.h"

int64_t getSize(Slice bkey, Slice ekey, leveldb::DB* db) {
    leveldb::Range ra;
    ra.start = convSlice(bkey);
    ra.limit = convSlice(ekey);
    uint64_t sz = 0;
    db->GetApproximateSizes(&ra, 1, &sz);
    return (int64_t)sz;
}

static void handleRange(leveldb::DB* db, HttpRequest& req, HttpResponse& resp) {
    Slice uri = req.uri;
    Slice rget = "/range-get";
    if (!uri.starts_with(rget)) {
        resp.setNotFound();
        return;
    }
    Slice bkey = uri.ltrim(rget.size());
    if (bkey.empty()) {
        bkey = "/";
    }
    Slice ekey = req.getArg("end");
    if (ekey.empty()) {
        ekey = "=";
    }
    bool inc = req.getArg("inc") == "1";
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    unique_ptr<leveldb::Iterator> rel1(it);
    int n = 0;
    leveldb::Slice lekey = convSlice(ekey);
    leveldb::Slice lbkey = convSlice(bkey);
    it->Seek(lbkey);
    if (!inc && it->key() == lbkey) {
        it->Next();
    }
    for (; it->Valid(); it->Next()) {
        if (it->key().compare(lekey) >= 0) {
            break;
        }
        leveldb::Slice k1(it->key());
        k1.remove_prefix(1);
        resp.body.append(k1.data(), k1.size());
        char buf[64];
        int cn = snprintf(buf, sizeof buf, " %lu\n", it->value().size());
        resp.body.append(buf, cn);
        resp.body.append(it->value().data(), it->value().size());
        resp.body.append("\n");
        if (++n >= g_batch_count || resp.body.size() >= (size_t)g_batch_size) {
            break;
        }
    }
}

static void handleNav(leveldb::DB* db, HttpRequest& req, HttpResponse& resp) {
    Slice uri = req.uri;
    Slice navn = "/nav-next";
    Slice navp = "/nav-prev";
    int n = 0;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    unique_ptr<leveldb::Iterator> rel1(it);
    string ln;
    resp.body.append("<a href=\"/nav-next/\">first-page</a></br>");
    if (uri.starts_with(navn)){
        Slice k = uri.ltrim(navn.size());
        Slice key = k;
        ln = util::format("<a href=\"/nav-prev%.*s\">prev-page</a><br/>",
            (int)key.size(), key.data());
        resp.body.append(ln);
        for (it->Seek(convSlice(k)); it->Valid(); it->Next()) {
            key = convSlice(it->key());
            key = key.ltrim(1);
            ln = util::format("<a href=\"%.*s?d=%.*s\">delete</a> <a href=\"/d/%.*s\">%.*s</a></br>",
                (int)uri.size(), uri.data(), (int)key.size(), key.data(),
                (int)key.size(), key.data(), (int)key.size(), key.data()); 
            resp.body.append(ln);
            if (++n>=g_page_limit) {
                break;
            }
        }
        ln = util::format("<a href=\"/nav-next/%.*s\">next-page</a></br>",
            (int)key.size(), key.data());
        resp.body.append(ln);
    } else if (uri.starts_with(navp)) {
        Slice k = uri.ltrim(navp.size());
        Slice key = k;
        vector<string> lns;
        ln = util::format("<a href=\"/nav-next%.*s\">next-page</a></br>",
            (int)key.size(), key.data());
        lns.push_back(ln);
        if (key[0] == '=') {
            it->SeekToLast();
        } else {
            it->Seek(convSlice(k));
        }
        for (; it->Valid(); it->Prev()) {
            k = convSlice(it->key());
            if (key[0] < '/') {
                break;
            }
            key = k.ltrim(1);
            ln = util::format("<a href=\"/d/%.*s\">%.*s</a></br>",
                (int)key.size(), key.data(), (int)key.size(), key.data()); 
            lns.push_back(ln);
            if (++n>=g_page_limit) {
                break;
            }
        }
        ln = util::format("<a href=\"/nav-prev/%.*s\">prev-page</a><br/>",
            (int)key.size(), key.data());
        lns.push_back(ln);
        for(auto it = lns.rbegin(); it != lns.rend(); it ++) {
            resp.body.append(*it);
        }
    } else {
        resp.setNotFound();
        return;
    }
    resp.body.append("<a href=\"/nav-prev=\">last-page</a></br>");
}

static void handleSize(leveldb::DB* db, HttpRequest& req, HttpResponse& resp) {
    Slice uri = req.uri;
    Slice pre = "/size";
    Slice bkey = uri.ltrim(pre.size());
    Slice ekey = req.getArg("end");
    if (ekey.empty()) {
        ekey = "=";
    }
    int64_t sz = getSize(bkey, ekey, db);
    resp.body = util::format("%ld", sz);
}

void handleReq(EventBase& base, LogDb* db, const TcpConnPtr& tcon) {
    HttpConn* con = HttpConn::asHttp(tcon);
    HttpRequest& req = con->getRequest();
    Status mst;
    HttpResponse& resp = con->getResponse();
    Slice uri = req.uri;
    Slice d = "/d/";
    string value;
    leveldb::DB* ldb = db->getdb();
    if (uri.starts_with(d)) {
        Slice localkey = uri.ltrim(d.size() - 1);
        leveldb::Slice key = convSlice(localkey);
        if (req.method == "GET") {
            leveldb::Status s = ldb->Get(leveldb::ReadOptions(), key, &value);
            if (s.ok()) {
                resp.body2 = value;
            } else if (s.IsNotFound()) {
                resp.setNotFound();
            } else {
                mst = (ConvertStatus)s;
            }
        } else if (req.method == "POST") {
            mst = db->write(localkey, req.getBody());
        } else if (req.method == "DELETE") {
            mst = db->remove(localkey);
        } else {
            resp.setStatus(403, "unknown method");
        }
        if (!mst.ok()) {
            resp.setStatus(500, "Internal Error");
            error("%.*s error %s", (int)req.method.size(), req.method.data(),
                    mst.toString().c_str());
        }
    } else if (uri.starts_with("/nav-")){
        string dk = req.getArg("d");
        if (dk.size()) {
            mst = db->remove("/"+dk);
        }
        if (!mst.ok()) {
            resp.setStatus(500, "Internal Error");
        } else {
            handleNav(ldb, req, resp);
        }
    } else if (uri.starts_with("/range-")){
        handleRange(ldb, req, resp);
    } else if (uri.starts_with("/size/")) {
        handleSize(ldb, req, resp);
    } else {
        resp.setNotFound();
    }
    info("req %s processed status %d length %lu",
        req.query_uri.c_str(), resp.status, resp.getBody().size());
    base.safeCall([tcon]{ HttpConn::asHttp(tcon)->sendResponse(); info("resp sended");});
}

