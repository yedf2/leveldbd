- [master-config](#master-config)
- [slave-config](#slave-config)
- [slave-status](#slave-status)
##master-config
```sh
#run the program as a daemon or not
#default on
daemon=off

#default info
loglevel=trace

#db data directory
dbdir=/root/ldbd

#default leveldbd.log
logfile=

#read threads number
#default 8
read_threads=8

#write threads number
#default 1
write_threads=1

#program bind ipv4 addr
#default 0.0.0.0
bind = 0.0.0.0

#program listen port
#default 80
port = 80

#stat-server listen port
#default 8080
stat_port = 8080

#limit records of a page
#default 1000
page_limit = 20

#limit records of batch operation
#default 100000
batch_count = 100

#limit size of a batch operation
#unit MB
#default 3
batch_size = 1

#limit size for binlog file
#unit MB
#default 0 do not write binlog
binlog_size = 64

#id of this db
#no default
dbid = 1

#help file
#default README
help_file = README.md
```

##slave-config
```sh
#run the program as a daemon or not
#default on
daemon=off

#default info
loglevel=trace

#db data directory
dbdir=/root/ldbd2

#default leveldbd.log
logfile=

#read threads number
#default 8
read_threads=8

#write threads number
#default 1
write_threads=1

#program bind ipv4 addr
#default 0.0.0.0
bind = 0.0.0.0

#program listen port
#default 80
port = 81

#stat-server listen port
#default 8080
stat_port = 8081

#limit records of a page
#default 1000
page_limit = 20

#limit records of batch operation
#default 100000
batch_count = 100

#limit size of a batch operation
#unit MB
#default 3
batch_size = 1

#limit size for binlog file
#unit MB
#default 0 do not write binlog
binlog_size = 0

#id of this db
#no default
dbid = 2

#help file
#default README
help_file = README.md
```

##slave-status
修改slave-status的内容，文件位置/root/ldbd/slave-status, dbdir下的slave-status
注意，该文件记录了从数据库同步的位置，会被数据库修改
slave-status内容为
```sh
localhost #host
80 #port
1 #binlog file no
0 #binlog offset
0 #data file finished flag
 #current key

```