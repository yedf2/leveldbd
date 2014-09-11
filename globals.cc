#include "globals.h"

Conf g_conf;
int g_page_limit;
int g_batch_count;
int g_batch_size;

void setGlobalConfig(Conf& conf) {
    g_page_limit = g_conf.getInteger("", "page_limit", 1000);
    g_batch_count = g_conf.getInteger("", "batch_count", 100*1000);
    g_batch_size = g_conf.getInteger("", "batch_size", 3*1024*1024);
}

