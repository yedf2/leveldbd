#include "handler.h"

Conf g_conf;
int g_page_limit;
int g_batch_count;
int g_batch_size;

void setGlobalConfig(Conf& conf) {
    g_page_limit = g_conf.getInteger("", "page_limit", 1000);
    g_batch_count = g_conf.getInteger("", "batch_count", 100*1000);
    g_batch_size = g_conf.getInteger("", "batch_size", 3*1024*1024);
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
        resp.body.append(it->key().data(), it->key().size());
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
            ln = util::format("<a href=\"/d%.*s\">%.*s</a></br>",
                (int)key.size(), key.data(), (int)key.size(), key.data()); 
            resp.body.append(ln);
            if (++n>=g_page_limit) {
                break;
            }
        }
        ln = util::format("<a href=\"/nav-next%.*s\">next-page</a></br>",
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
            key = k;
            ln = util::format("<a href=\"/d%.*s\">%.*s</a></br>",
                (int)key.size(), key.data(), (int)key.size(), key.data()); 
            lns.push_back(ln);
            if (++n>=g_page_limit) {
                break;
            }
        }
        ln = util::format("<a href=\"/nav-prev%.*s\">prev-page</a><br/>",
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


void handleReq(EventBase& base, leveldb::DB* db, const TcpConnPtr& tcon) {
    HttpConn* con = HttpConn::asHttp(tcon);
    HttpRequest& req = con->getRequest();
    leveldb::Status s;
    HttpResponse& resp = con->getResponse();
    Slice uri = req.uri;
    Slice d = "/d/";
    string value;
    if (uri.starts_with(d)) {
        leveldb::Slice key = convSlice(uri.ltrim(d.size() - 1));
        if (req.method == "GET") {
            s = db->Get(leveldb::ReadOptions(), key, &value);
            if (s.ok()) {
                resp.body2 = value;
            } else {
                resp.setNotFound();
            }
        } else if (req.method == "POST") {
            leveldb::Slice v = convSlice(req.getBody());
            s = db->Put(leveldb::WriteOptions(), key, v);
        } else if (req.method == "DELETE") {
            s = db->Delete(leveldb::WriteOptions(), key);
        } else {
            resp.setStatus(403, "unknown method");
        }
        if (s.IsNotFound()) {
            resp.setNotFound();
        } else if (!s.ok()) {
            resp.setStatus(500, "Internal Error");
        }
    } else if (uri.starts_with("/nav-")){
        handleNav(db, req, resp);
    } else if (uri.starts_with("/range-")){
        handleRange(db, req, resp);
    }
    info("req %s processed status %d length %lu",
        req.query_uri.c_str(), resp.status, resp.getBody().size());
    base.safeCall([tcon]{ HttpConn::asHttp(tcon)->sendResponse(); info("resp sended");});
}

