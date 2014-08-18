#include <handy.h>
#include <daemon.h>
#include <http.h>
#include <stat-server.h>
#include <logging.h>
#include <conf.h>
#include <thread_util.h>
#include "leveldb/db.h"

using namespace std;
using namespace handy;

void setupStatServer(StatServer& svr, EventBase& base, const char* argv[]);
void handleHttpReq(EventBase& base, leveldb::DB* db, const HttpConn& con, ThreadPool& rpool, ThreadPool& wpool);
void processArgs(int argc, const char* argv[], Conf& conf);
leveldb::Status setupDb(leveldb::DB*& db, Conf& conf);
leveldb::Slice convSlice(Slice s) { return leveldb::Slice(s.data(), s.size()); }
Slice convSlice(leveldb::Slice s) { return Slice(s.data(), s.size()); }

int main(int argc, const char* argv[]) {
    string program = argv[0];
    Conf conf;
    processArgs(argc, argv, conf);

    string logfile = conf.get("", "logfile", program+".log");
    if (logfile.size()) {
        Logger::getLogger().setFileName(logfile.c_str());
    }
    Logger::getLogger().setLogLevel(conf.get("", "loglevel", "INFO").c_str());

    leveldb::DB* db = NULL;
    leveldb::Status s = setupDb(db, conf);
    fatalif(!s.ok(), "leveldb open failed: %s", s.ToString().c_str());
    unique_ptr<leveldb::DB> release1(db);

    ThreadPool readPool(conf.getInteger("", "read_threads", 8));
    ThreadPool writePool(conf.getInteger("", "write_threads", 1));

    EventBase base(1000, 100*1000);
    HttpServer leveldbd(&base, Ip4Addr(80));
    leveldbd.onDefault([&, db](const HttpConn& con) { handleHttpReq(base, db, con, readPool, writePool);});
    StatServer statsvr(&base, Ip4Addr(8080));
    setupStatServer(statsvr, base, argv);
    Signal::signal(SIGINT, [&]{base.exit(); });
    base.loop();
    readPool.exit();
    readPool.join();
    writePool.exit();
    writePool.join();
    return 0;
}

void handleReq(EventBase& base, leveldb::DB* db, const HttpConn& con) {
    HttpRequest& req = con.getRequest();
    leveldb::Status s;
    HttpResponse resp;
    Slice uri = req.uri;
    Slice d = "/d/";
    string value;
    if (uri.starts_with(d)) {
        leveldb::Slice key = convSlice(uri.ltrim(3));
        if (req.method == "GET") {
            s = db->Get(leveldb::ReadOptions(), key, &value);
            if (s.ok()) {
                resp.body2 = value;
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
    } else {
        con.close();
    }
    info("req %s processed", req.query_uri.c_str());
    con.clearRequest();
    resp.encode(con.con->getOutput());
    base.safeCall([=]{con.con->sendOutput(); info("req sended");});
}

void handleHttpReq(EventBase& base, leveldb::DB* db, const HttpConn& con, ThreadPool& rpool, ThreadPool& wpool){
    HttpRequest& req = con.getRequest();
    ThreadPool* pool = req.method == "GET" ? &rpool: &wpool;
    pool->addTask([=, &base] { handleReq(base, db, con); });
}

leveldb::Status setupDb(leveldb::DB*& db, Conf& conf){
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, conf.get("", "dbdir", "ldb"), &db);
    return s;
}

void processArgs(int argc, const char* argv[], Conf& conf){
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
    int r = conf.parse(config.c_str());
    if (r != 0) {
        printf("config %s parse error at line %d", config.c_str(), r);
        exit(1);
    }
    string pidfile = conf.get("", "pidfile", argv[0]+(string)".pid");
    if (conf.getBoolean("", "daemon", true)) {
        Daemon::daemonProcess(cmd.c_str(), pidfile.c_str());
    }
}

void setupStatServer(StatServer& svr, EventBase& base, const char* argv[]) {
    svr.onState("loglevel", "log level for server", []{return Logger::getLogger().getLogLevelStr(); });
    svr.onState("pid", "process id of server", [] { return util::format("%d", getpid()); });
    svr.onCmd("lesslog", "set log to less detail", []{ Logger::getLogger().adjustLogLevel(-1); return "OK"; });
    svr.onCmd("morelog", "set log to more detail", [] { Logger::getLogger().adjustLogLevel(1); return "OK"; });
    svr.onCmd("restart", "restart program", [&] { 
        base.safeCall([&]{ base.exit(); Daemon::changeTo(argv);}); 
        return "restarting"; 
    });
    svr.onCmd("stop", "stop program", [&] { base.safeCall([&]{base.exit();}); return "stoping"; });
    svr.onPage("page", "show page content", [] { return "this is a page"; });
}
