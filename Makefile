CC=cc
CXX=g++
CXXFLAGS= -DOS_LINUX -g -std=c++11 -Wall -I. -I../handy/handy -Ideps/leveldb/include
LDFLAGS= -pthread ../handy/handy/libhandy.a deps/leveldb/libleveldb.a deps/snappy/.libs/libsnappy.a

SOURCES = leveldbd.cc handler.cc

OBJECTS = $(SOURCES:.cc=.o)

PROGRAM = leveldbd

default: $(PROGRAM)

clean:
	-rm -f $(PROGRAM)
	-rm -f *.o

$(PROGRAM): $(OBJECTS) ../handy/handy/libhandy.a
	$(CXX) -o $@ $(OBJECTS) $(LDFLAGS)

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@
