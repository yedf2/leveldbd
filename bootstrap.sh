echo git submodule update
git submodule foreach git pull origin master
echo making snappy
cd deps/snappy; 
if [ -f Makefile ]; then 
    make clean && make || exit 1
else
    ./autogen.sh && ./configure && make clean && make || exit 1
fi
cd ../..
echo making leveldb
cd deps/leveldb; make clean && CXXFLAGS="$CXXFLAGS -I../snappy" make libleveldb.a || exit 1; cd ../..
echo making handy
cd deps/handy; ./build_detect_platform && make clean && make || exit 1; cd ../..