#include <daemon.h>
#include <stat-server.h>
#include <thread_util.h>
#include "handler.h"
#include <status.h>
#include <file.h>
#include "globals.h"
#include "logdb.h"

int main(int argc, const char* argv[]) {
    if (argc < 2) {
        printf("usage: %s <binlog file>\n", argv[0]);
        return 1;
    }
    Status st = LogDb::dumpFile(argv[1]);
    if (!st.ok()) {
        error("dumpfile error: %d %s", st.code(), st.msg());
        return 1;
    }
    return 0;
}

