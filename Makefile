CC=cc
CXX=g++
CXXFLAGS= -DOS_LINUX -g -std=c++11 -Wall -I. -I../handy/handy -Ideps/leveldb/include
LDFLAGS= -pthread ../handy/handy/libhandy.a deps/leveldb/libleveldb.a deps/snappy/.libs/libsnappy.a

SOURCES = handler.cc globals.cc logdb.cc logfile.cc

PROGRAMS = leveldbd dumplog

OBJECTS = $(SOURCES:.cc=.o)

default: $(PROGRAMS)

clean:
	-rm -f $(PROGRAMS)
	-rm -f *.o

$(PROGRAMS): $(OBJECTS) ../handy/handy/libhandy.a

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc:
	$(CXX) $< -o $@ $(OBJECTS) $(CXXFLAGS) $(LDFLAGS)
