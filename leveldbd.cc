#include <daemon.h>
#include <stat-server.h>
#include <thread_util.h>
#include "handler.h"
#include <status.h>
#include <file.h>
#include "globals.h"

void setupStatServer(StatServer& svr, EventBase& base, leveldb::DB* db, const char* argv[]);
void handleHttpReq(EventBase& base, LogDb* db, const TcpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool);
void processArgs(int argc, const char* argv[], Conf& conf);

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
    leveldbd.onDefault([&](HttpConn* con) {
        TcpConnPtr tcon = con->asTcp(); //add refcount to avoid connection released
        handleHttpReq(base, &db, tcon, readPool, writePool);
    });
    setupStatServer(statsvr, base, db.getdb(), argv);

    Signal::signal(SIGINT, [&]{base.exit(); });
    base.loop();
    readPool.exit().join();
    writePool.exit().join();
    return 0;
}

void handleHttpReq(EventBase& base, LogDb* db, const TcpConnPtr& con, ThreadPool& rpool, ThreadPool& wpool){
    HttpRequest& req = HttpConn::asHttp(con)->getRequest();
    ThreadPool* pool = req.method == "GET" ? &rpool: &wpool;
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
}

