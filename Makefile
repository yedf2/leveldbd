CC=cc
CXX=g++
CXXFLAGS= -DOS_LINUX -g -std=c++11 -Wall -I. -I../handy/handy -Ideps/leveldb/include
LDFLAGS= -pthread -L../handy/handy -lhandy deps/leveldb/libleveldb.a deps/snappy/.libs/libsnappy.a

SOURCES = leveldbd.cc

OBJECTS = $(SOURCES:.cc=.o)

PROGRAM = leveldbd

default: $(PROGRAM)

clean:
	-rm -f $(PROGRAM)
	-rm -f *.o

$(PROGRAM): $(OBJECTS)
	$(CXX) -o $@ $(OBJECTS) $(LDFLAGS)

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@
