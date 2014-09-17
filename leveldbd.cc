#include <daemon.h>
#include <stat-server.h>
#include <thread_util.h>
#include "handler.h"
#include <status.h>
#include <file.h>
#include "globals.h"

void setupStatServer(StatServer& svr, EventBase& base, leveldb::DB* db, const char* argv[]);
void handleHttpReq(EventBase& base, LogDb* db, const HttpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool);
void processArgs(int argc, const char* argv[], Conf& conf);

Status decodeRangeResp(Slice* body, Slice* key, Slice* value) {
    Status inval = Status::fromFormat(EINVAL, "bad format for range resp");
    if (body->empty()) {
        error("empty body in decode");
        return inval;
    }
    for (const char* p = body->begin(); p < body->end(); p++) {
        if (*p == ' ') {
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
                    *value = Slice(pe, pe+len);
                    *body = Slice(pe, body->end());
                    return Status();
                }
            }
        }
    }
    error("bad format in range format, no space");
    return inval;
}

Status decodeBinlogResp(Slice* body, Slice* record) {
    Status inval = Status::fromFormat(EINVAL, "bad format for binlog resp");
    if (body->empty()) {
        error("empty body in binlog resp");
        return inval;
    }
    int64_t magic = *(int64_t*)body->begin();
    if (magic != LOG_MAGIC) {
        error("bad magic no in binlog resp");
        return inval;
    }
    int64_t len = *(int64_t*)(body->begin()+8);
    len = net::ntoh(len);
    if (body->begin()+8+len > body->end()) {
        error("bad length in binlog resp len %ld body %ld", len, body->size());
        return inval;
    }
    *record = Slice(body->begin()+8, len);
    *body = Slice(body->begin()+8+len, body->end());
    return Status();
}

void sendSyncReq(LogDb* db, EventBase* base, const HttpConnPtr& con) {
    SlaveStatus* ss = &db->slaveStatus;
    HttpRequest& req = con->getRequest();
    if (ss->key != "=") {
        req.query_uri = "/range-get" + ss->key;
    } else {
        req.query_uri = util::format("/binlog/?f=%05ld&off=%ld", ss->fileno, ss->offset);
    }
    debug("geting %s", req.query_uri.c_str());
    base->safeCall([con] { con->sendRequest();});
}

void processSyncResp(LogDb* db, const HttpConnPtr& con, EventBase* base) {
    ExitCaller atend([=]{ sendSyncReq(db, base, con); });

    HttpResponse& res = con->getResponse();
    Slice body = res.getBody();
    string reqinfo = res.getHeader("req-info");
    vector<Slice> fs = Slice(reqinfo).split(' ');
    if (fs.size() != 3) {
        error("unexpected header req-info %s", reqinfo.data());
        return;
    }
    SlaveStatus* ss = &db->slaveStatus;
    Slice key = fs[0];
    int64_t fno = util::atoi(fs[1].begin(), fs[1].end());
    int64_t off = util::atoi(fs[2].begin(), fs[2].end());
    if (key != ss->key || fno != ss->fileno || off != ss->offset) {
        error("header req-info not match slave status %s %ld %ld", ss->key.c_str(), ss->fileno, ss->offset);
        return;
    }

    Status st;
    if (key != "=") { //range-get resp
        Slice key, value;
        while (body.size() && (st=decodeRangeResp(&body, &key, &value), st.ok())) {
            st = db->write(key, value);
            if (!st.ok()) {
                break;
            }
        }
    } else { //binlog resp
        Slice record;
        while (body.size() && (st=decodeBinlogResp(&body, &record), st.ok())) {
            st = db->applyLog(record);
            if (!st.ok()) {
                break;
            }
        }
    }
    if (st.ok()) {
        string nextinfo = res.getHeader("next-info");
        vector<Slice> ns = Slice(nextinfo).split(' ');
        if (ns.size() != 3) {
            error("unexpected header next-info %s", nextinfo.c_str());
        } else {
            ss->key = ns[0];
            ss->fileno = util::atoi(ns[1].begin(), ns[1].end());
            ss->offset = util::atoi(ns[2].begin(), ns[2].end());
        }
    }
}

void httpConnectTo(ThreadPool* wpool, LogDb* db, EventBase* base, const string& ip, int port) {
    HttpConnPtr con = HttpConn::connectTo(base, ip, port, 200);
    con->onState([=](const TcpConnPtr& con) {
        TcpConn::State st = con->getState();
        HttpConnPtr hcon = con;
        if (st == TcpConn::Connected) {
            wpool->addTask([=]{sendSyncReq(db, base, con); });
        } else if (st == TcpConn::Failed || st == TcpConn::Closed) {
            base->runAfter(3000, [=]{ httpConnectTo(wpool, db, base, ip, port); });
        }
    });

    con->onMsg([=](const HttpConnPtr& hcon) {
        wpool->addTask([=]{
            processSyncResp(db, hcon, base);
        });
    });
}

int main(int argc, const char* argv[]) {
    string program = argv[0];
    processArgs(argc, argv, g_conf);

    //setup log
    string logfile = g_conf.get("", "logfile", program+".log");
    if (logfile.size()) {
        Logger::getLogger().setFileName(logfile.c_str());
    }
    string loglevel = g_conf.get("", "loglevel", "INFO");
    Logger::getLogger().setLogLevel(loglevel);

    //setup thread pool
    ThreadPool readPool(g_conf.getInteger("", "read_threads", 8));
    ThreadPool writePool(1);

    //setup db
    setGlobalConfig(g_conf);
    LogDb db;
    Status st = db.init(g_conf);
    fatalif(!st.ok(), "LogDb init failed. %s", st.msg());

    //setup network
    string ip = g_conf.get("", "bind", "");
    int port = g_conf.getInteger("", "port", 80);
    int stat_port = g_conf.getInteger("", "stat_port", 8080);
    EventBase base(1000);
    HttpServer leveldbd(&base, ip, port);
    StatServer statsvr(&base, ip, stat_port);
    leveldbd.onDefault([&](const HttpConnPtr& con) {
        handleHttpReq(base, &db, con, readPool, writePool);
    });
    setupStatServer(statsvr, base, db.getdb(), argv);

    if (db.slaveStatus.isValid()) {
        httpConnectTo(&writePool, &db, &base, db.slaveStatus.host, db.slaveStatus.port);
    }
    Signal::signal(SIGINT, [&]{base.exit(); });
    base.loop();
    readPool.exit().join();
    writePool.exit().join();
    return 0;
}

void handleHttpReq(EventBase& base, LogDb* db, const HttpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool){
    HttpRequest& req = con->getRequest();
    ThreadPool* pool = req.method == "GET" && !Slice(req.uri).starts_with("/nav-")? &rpool: &wpool;
    pool->addTask([=, &base] { handleReq(base, db, con); });
}

void processArgs(int argc, const char* argv[], Conf& g_conf){
    string config = argv[0] + string(".conf");
    const char* usage = "usage: %s [-f config_file] [start|stop|restart]\n";
    char* const* gv = (char* const*)argv;
    for (int ch=0; (ch=getopt(argc, gv, "f:h"))!= -1;) {
        switch(ch) {
        case 'f':
            config = optarg;
            break;
        case 'h':
            printf(usage, argv[0]);
            exit(0);
            break;
        default:
            printf("unknown option %c\n", ch);
            printf(usage, argv[0]);
            exit(1);
        }
    }
    string cmd = "start";
    if (argc > optind) {
        cmd = argv[optind];
    }
    if (argc > optind + 1 || (cmd != "start" && cmd != "stop" && cmd != "restart")) {
        printf(usage, argv[0]);
        exit(1);
    }
    int r = g_conf.parse(config.c_str());
    if (r != 0) {
        printf("config %s parse error at line %d", config.c_str(), r);
        exit(1);
    }
    string pidfile = g_conf.get("", "pidfile", argv[0]+(string)".pid");
    if (g_conf.getBoolean("", "daemon", true)) {
        Daemon::daemonProcess(cmd.c_str(), pidfile.c_str());
    }
}

void setupStatServer(StatServer& svr, EventBase& base, leveldb::DB* db, const char* argv[]) {
    svr.onState("loglevel", "log level for server", []{return Logger::getLogger().getLogLevelStr(); });
    svr.onState("pid", "process id of server", [] { return util::format("%d", getpid()); });
    svr.onState("space", "total space of db kB", [db] { return util::format("%ld", getSize("/", "=", db)/1024); });
    svr.onCmd("lesslog", "set log to less detail", []{ Logger::getLogger().adjustLogLevel(-1); return "OK"; });
    svr.onCmd("morelog", "set log to more detail", [] { Logger::getLogger().adjustLogLevel(1); return "OK"; });
    svr.onCmd("restart", "restart program", [&] { 
        base.safeCall([&]{ base.exit(); Daemon::changeTo(argv);}); 
        return "restarting"; 
    });
    svr.onCmd("stop", "stop program", [&] { base.safeCall([&]{base.exit();}); return "stoping"; });
    svr.onPage("config", "show config file", [] { 
        string cont;
        Status st = file::getContent(g_conf.filename, cont);
        return st.code() ? "Not Found" : cont;
    });
    svr.onPage("help", "show help", []{
        return
            "navigate forward: localhost/nav-next/\n"
            "navigate backward: localhost/nav-prev/\n"
            "kv get: localhost/d/key1\n"
            "kv set: curl localhost/d/key1 -d\"value1\"\n"
            "kv del: curl -XDELETE localhost/d/key1\n"
            "";
    });
}

