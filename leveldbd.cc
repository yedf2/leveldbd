#include <handy/daemon.h>
#include <handy/stat-svr.h>
#include <handy/threads.h>
#include "handler.h"
#include <handy/status.h>
#include <handy/file.h>
#include "globals.h"
#include "binlog-msg.h"

void setupStatServer(StatServer& svr, EventBase& base, LogDb* db, const char* argv[]);
void handleHttpReq(EventBase& base, LogDb* db, const HttpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool);
void processArgs(int argc, const char* argv[], Conf& conf);
void httpConnectTo(ThreadPool* wpool, LogDb* db, EventBase* base, const string& ip, int port);

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

    info("program begin. loglevel %s", loglevel.c_str());
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
    HttpServer leveldbd(&base);
    int r = leveldbd.bind(ip, port);
    exitif(r, "bind failed %d %s", errno, strerror(errno));
    StatServer statsvr(&base);
    r = statsvr.bind(ip, stat_port);
    exitif(r, "bind failed %d %s", errno, strerror(errno));
    leveldbd.onDefault([&](const HttpConnPtr& con) {
        handleHttpReq(base, &db, con, readPool, writePool);
    });
    base.runAfter(3000, [&]{ sendEmptyBinlog(&base, &db); }, 5000);
    setupStatServer(statsvr, base, &db, argv);

    if (db.slaveStatus_.isValid()) {
        httpConnectTo(&writePool, &db, &base, db.slaveStatus_.host, db.slaveStatus_.port);
    }
    Signal::signal(SIGINT, [&]{base.exit(); });
    base.loop();
    readPool.exit().join();
    writePool.exit().join();
    return 0;
}

void handleHttpReq(EventBase& base, LogDb* db, const HttpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool){
    HttpRequest& req = con.getRequest();
    ThreadPool* pool = req.method == "GET" && !Slice(req.uri).starts_with("/nav-")? &rpool: &wpool;
    pool->addTask([=, &base] { handleReq(base, db, con); });
}

void httpConnectTo(ThreadPool* wpool, LogDb* db, EventBase* base, const string& ip, int port) {
    HttpConnPtr con = TcpConn::createConnection(base, ip, port, 200);
    con->onState([=](const TcpConnPtr& con) {
        TcpConn::State st = con->getState();
        HttpConnPtr hcon = con;
        if (st == TcpConn::Connected) {
            wpool->addTask([=]{sendSyncReq(db, base, con); });
        } else if (st == TcpConn::Failed || st == TcpConn::Closed) {
            base->runAfter(3000, [=]{ httpConnectTo(wpool, db, base, ip, port); });
        }
    });

    con.onHttpMsg([=](const HttpConnPtr& hcon) {
        wpool->addTask([=]{
            processSyncResp(db, hcon, base);
        });
    });
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

void setupStatServer(StatServer& svr, EventBase& base, LogDb* db, const char* argv[]) {
    svr.onState("loglevel", "log level for server", []{return Logger::getLogger().getLogLevelStr(); });
    svr.onState("pid", "process id of server", [] { return getpid(); });
    svr.onState("space", "total space of db kB", [db] { return getSize("/", "=", db->getdb())/1024; });
    svr.onState("dbid", "dbid of this db", [db] { return db->dbid_; });
    svr.onState("binlog-file", "current binlog file no of this db", [db] { return db->lastFile_; });
    svr.onState("binlog-offset", "current binlog file offset", [db] { 
        size_t sz = 0;
        Status st = file::getFileSize(db->binlogDir_+FileName::binlogFile(db->lastFile_), &sz);
        return sz;
    });
    svr.onState("slave-current-key", "slave key of this db", [db] { return db->getSlaveStatusLock().pos.key; });
    svr.onState("slave-file", "slave file of this db", [db] { return db->getSlaveStatusLock().pos.fileno; });
    svr.onState("slave-offset", "slave offset of this db", [db] { return db->getSlaveStatusLock().pos.offset; });
    svr.onCmd("lesslog", "set log to less detail", []{ Logger::getLogger().adjustLogLevel(-1); return "OK"; });
    svr.onCmd("morelog", "set log to more detail", [] { Logger::getLogger().adjustLogLevel(1); return "OK"; });
    svr.onCmd("restart", "restart program", [&] { 
        base.safeCall([&]{ base.exit(); Daemon::changeTo(argv);}); 
        return "restarting"; 
    });
    svr.onCmd("stop", "stop program", [&] { base.safeCall([&]{base.exit();}); return "stoping"; });
    svr.onPageFile("config", "show config file", g_conf.filename);
    svr.onPageFile("help", "show help", g_conf.get("", "help_file", "README"));
}

