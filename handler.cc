#include "handler.h"
#include "binlog-msg.h"

void addKvBody(Slice key, const Slice* value, string* body) {
    body->append(key.data(), key.size());
    char buf[64];
    int cn = snprintf(buf, sizeof buf, "\n%ld\n", value ? (int64_t)value->size() : -1);
    body->append(buf, cn);
    body->append(value->data(), value->size());
    body->append("\n");

}

Status decodeKvBody(Slice* body, Slice* key, Slice* value, bool* exists) {
    Status inval = Status::fromFormat(EINVAL, "bad format for range resp");
    if (body->empty()) {
        error("empty body in decode");
        return inval;
    }
    for (const char* p = body->begin(); p < body->end(); p++) {
        if (*p == '\n') {
            *key = Slice(body->begin(), p);
            p++;
            for (const char* pe = p; pe < body->end(); pe++) {
                if (*pe == '\n') {
                    int64_t len = util::atoi(p, pe);
                    pe++;
                    if (pe + len > body->end()) {
                        error("bad format for range resp");
                        return inval;
                    }
                    if (len == -1) {
                        *exists = false;
                    } else {
                        *exists = true;
                        *value = Slice(pe, pe+len);
                        *body = Slice(pe+len+1, body->end());
                    }
                    return Status();
                }
            }
        }
    }
    error("bad format in range format, no line feed");
    return inval;
}

void addKeyBody(Slice key, string* body) {
    body->append(key.data(), key.size());
    *body += '\n';
}

Status decodeKeyBody(Slice* body, Slice* key) {
    Status inval = Status::fromFormat(EINVAL, "bad format for range resp");
    if (body->empty()) {
        error("empty body in decode");
        return inval;
    }
    for (const char* p = body->begin(); p < body->end(); p++) {
        if (*p == '\n') {
            *key = Slice(body->begin(), p);
            *body = Slice(p+1, body->end());
            return Status();
        }
    }
    error("bad format in range format, no line feed");
    return inval;
}

static void handleBatchGet(LogDb* db, HttpRequest& req, HttpResponse& resp) {
    Slice key;
    Status st;
    leveldb::DB* ldb = db->getdb();
    Slice body = req.getBody();
    while (body.size() && st.ok() && (st=decodeKeyBody(&body, &key), st.ok())) {
        string value;
        leveldb::Status s = ldb->Get(leveldb::ReadOptions(), convSlice(key), &value);
        if (s.ok()) {
            Slice v(value);
            addKvBody(key, &v, &resp.body);
        } else if (s.IsNotFound()) {
            addKvBody(key, NULL, &resp.body);
        } else {
            error("ldb error: %s", s.ToString().c_str());
            st = (ConvertStatus)s;
        }
    }
    if (!st.ok()) {
        resp.setStatus(500, "Internal Error");
    }
}

static void handleBatchSet(LogDb* db, HttpRequest& req, HttpResponse& resp) {
    Slice key, value;
    Status st;
    Slice body = req.getBody();
    bool exists;
    while (body.size() && st.ok() && (st=decodeKvBody(&body, &key, &value, &exists), st.ok())) {
        st = db->write(key, value);
    }
    if (!st.ok()) {
        resp.setStatus(500, "Internal Error");
    }
}

static void handleBatchDelete(LogDb* db, HttpRequest& req, HttpResponse& resp) {
    Slice key;
    Status st;
    Slice body = req.getBody();
    while (body.size() && (st=decodeKeyBody(&body, &key), st.ok())) {
        st = db->remove(key);
        if (!st.ok()) {
            break;
        }
    }
    if (!st.ok()) {
        resp.setStatus(500, "Internal Error");
    }
}

static void handleRangeGet(LogDb* db, HttpRequest& req, HttpResponse& resp) {
    leveldb::DB* ldb = db->getdb();
    Slice uri = req.uri;
    Slice rget = "/range-get/";
    if (!uri.starts_with(rget)) {
        resp.setNotFound();
        return;
    }
    Slice bkey = uri.sub(rget.size());
    Slice ekey = req.getArg("end");
    if (ekey.empty()) {
        ekey = "\xff";
    }
    bool inc = req.getArg("inc") == "1";
    leveldb::Iterator* it = ldb->NewIterator(leveldb::ReadOptions());
    unique_ptr<leveldb::Iterator> rel1(it);
    int n = 0;
    leveldb::Slice lekey = convSlice(ekey);
    leveldb::Slice lbkey = convSlice(bkey);
    it->Seek(lbkey);
    if (!inc && it->Valid() && it->key() == lbkey) {
        it->Next();
    }
    Slice k1;
    for (; it->Valid(); it->Next()) {
        if (it->key().compare(lekey) >= 0) {
            break;
        }
        k1 = convSlice(it->key());
        Slice v = convSlice(it->value());
        addKvBody(k1, &v, &resp.body);
        if (++n >= g_batch_count || resp.body.size() >= (size_t)g_batch_size) {
            break;
        }
    }
    addBinlogHeader(bkey, k1, req, resp);
}

int64_t getSize(Slice bkey, Slice ekey, leveldb::DB* db) {
    leveldb::Range ra;
    ra.start = convSlice(bkey);
    ra.limit = convSlice(ekey);
    uint64_t sz = 0;
    db->GetApproximateSizes(&ra, 1, &sz);
    return (int64_t)sz;
}

static void handleNav(leveldb::DB* db, HttpRequest& req, HttpResponse& resp) {
    Slice uri = req.uri;
    Slice navn = "/nav-next/";
    Slice navp = "/nav-prev/";
    Slice navl = "/nav-prev=";
    int n = 0;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    unique_ptr<leveldb::Iterator> rel1(it);
    string ln;
    resp.body.append("<a href=\"/nav-next/\">first-page</a></br>");
    if (uri.starts_with(navn)){
        Slice pgkey = uri.sub(navn.size());
        ln = util::format("<a href=\"/nav-prev/%.*s\">prev-page</a><br/>",
            (int)pgkey.size(), pgkey.data());
        resp.body.append(ln);
        Slice key = pgkey;
        for (it->Seek(convSlice(pgkey)); it->Valid(); it->Next()) {
            key = convSlice(it->key());
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
        Slice pgkey = uri.sub(navp.size());
        vector<string> lns;
        ln = util::format("<a href=\"/nav-next/%.*s\">next-page</a></br>",
            (int)pgkey.size(), pgkey.data());
        lns.push_back(ln);
        Slice key = pgkey;
        for (it->Seek(convSlice(pgkey)); it->Valid(); it->Prev()) {
            key = convSlice(it->key());
            ln = util::format("<a href=\"%.*s?d=%.*s\">delete</a> <a href=\"/d/%.*s\">%.*s</a></br>",
                (int)uri.size(), uri.data(), (int)key.size(), key.data(),
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
    } else if (uri == navl) {
        Slice key;
        vector<string> lns;
        for (it->SeekToLast(); it->Valid(); it->Prev()) {
            key = convSlice(it->key());
            if (lns.empty()) {
                ln = util::format("<a href=\"/nav-next/%.*s\">next-page</a></br>",
                    (int)key.size(), key.data());
                lns.push_back(ln);
            }
            ln = util::format("<a href=\"%.*s?d=%.*s\">delete</a> <a href=\"/d/%.*s\">%.*s</a></br>",
                (int)uri.size(), uri.data(), (int)key.size(), key.data(),
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
    Slice pre = "/size/";
    Slice bkey = uri.sub(pre.size());
    Slice ekey = req.getArg("end");
    if (ekey.empty()) {
        ekey = "\xff";
    }
    int64_t sz = getSize(bkey, ekey, db);
    resp.body = util::format("%ld", sz);
}

void handleReq(EventBase& base, LogDb* db, const HttpConnPtr& con) {
    HttpRequest& req = con->getRequest();
    Status mst;
    HttpResponse& resp = con->getResponse();
    Slice uri = req.uri;
    Slice d = "/d/";
    string value;
    leveldb::DB* ldb = db->getdb();
    if (uri.starts_with(d)) {
        Slice localkey = uri.sub(d.size());
        leveldb::Slice key = convSlice(localkey);
        if (key.empty()) {
            resp.setStatus(403, "empty key");
        } else if (req.method == "GET") {
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
    } else if (uri.starts_with("/batch-get/")) {
        handleBatchGet(db, req, resp);
    } else if (uri.starts_with("/batch-set/")) {
        handleBatchSet(db, req, resp);
    } else if (uri.starts_with("/batch-delete/")) {
        handleBatchDelete(db, req, resp);
    } else if (uri.starts_with("/range-get/")){
        handleRangeGet(db, req, resp);
    } else if (uri.starts_with("/size/")) {
        handleSize(ldb, req, resp);
    } else if (uri.starts_with("/binlog/")) {
        handleBinlog(db, &base, con);
        return;
    } else {
        resp.setNotFound();
    }
    info("req %s processed status %d length %lu",
        req.query_uri.c_str(), resp.status, resp.getBody().size());
    base.safeCall([con]{ con->sendResponse(); info("resp sended");});
}

