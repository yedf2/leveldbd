#!/bin/sh
test -e deps && exit 0
mkdir -p deps; cd deps

git clone https://github.com/google/snappy &&
cd snappy && ./autogen.sh && ./configure && make clean && make $1 || exit 1
cd ..

git clone https://github.com/yedf/leveldb &&
cd leveldb && make libleveldb.a $1 || exit 1
cd ..

git clone https://github.com/yedf/handy &&
cd handy && make $1 || exit 1
