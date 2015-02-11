**leveldbd是一个nosql数据库，底层使用leveldb作为存储引擎，提供REST接口。**

提供的特性包括

- 主从同步
- 主主同步
- snappy压缩
- 范围查询
- 批量读写
- 易于管理
- 内置状态查看与管理

使用了C++11，需要g++4.8 在ubuntu14上测试

##编译运行

git clone https://github.com/yedf/leveldbd.git

cd leveldbd

make

./leveldbd

##主从复制

https://github.com/yedf/leveldbd/blob/master/master-slave.md

##leveldbd REST接口

###Get

curl localhost/d/key1


###Set

curl -d"value1" localhost/d/key1


###Delete

curl -X"DELETE" localhost/d/key1


###Navigate 网页方式进行浏览与管理

localhost/nav-next/begin-key


###Batch-Get

localhost/batch-get/

request body data format is key-format. (format detail can be found in the end)

response data format is kv-format


###Batch-Set

curl -X"POST" localhost/batch-set/

request body data format is kv-format.


###Batch-Delete

curl -X"DELETE" localhost/batch-delete/

request body data format is key-format


###Range-Get

localhost/range-get/begin-key?end=end-key&inc=1

query 'end' is optional which specify the end key (excluded in response), default get untill end

query 'inc' is optional which specify whether the begin key should be included in response. default 0

response data format is kv-format.


###body format
kv-format:'[key]\n[value len]\n[value]\n[key2]\n0\n\n[key3]\n-1\n\n[key4]...'

    value len:

        -1 indicate key not exist

        0  value lenght is 0


key-format: '[key1]\n[key2]...'
