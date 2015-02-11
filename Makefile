$(shell ./bootstrap.sh 1>&2)
CC=cc
CXX=g++
CXXFLAGS= -DOS_LINUX -g -std=c++11 -Wall -I. -Ideps/handy/handy -Ideps/leveldb/include
LDFLAGS= -pthread deps/handy/handy/libhandy.a deps/leveldb/libleveldb.a deps/snappy/.libs/libsnappy.a

SOURCES = handler.cc globals.cc logdb.cc logfile.cc binlog-msg.cc

PROGRAMS = leveldbd dumplog

OBJECTS = $(SOURCES:.cc=.o)

default: $(PROGRAMS) db2

db2: leveldbd
	cp -f leveldbd db2

clean:
	-rm -f $(PROGRAMS)
	-rm -f *.o

$(PROGRAMS): $(OBJECTS) deps/handy/handy/libhandy.a

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc:
	$(CXX) $< -o $@ $(OBJECTS) $(CXXFLAGS) $(LDFLAGS)
