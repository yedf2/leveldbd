Get
http://localhost/d/key1

Set
curl -d"value1" localhost/d/key1

Delete
curl -X"DELETE" localhost/d/key1

Navigate
localhost/nav-next/begin-key

Batch-Get
localhost/batch-get/
request body data format is key-format. (format detail can be found in the end)
response data format is kv-format

Batch-Set
curl -X"POST" localhost/batch-set/
request body data format is kv-format.

Batch-Delete
curl -X"DELETE" localhost/batch-delete/
request body data format is key-format

Range-Get
localhost/range-get/begin-key?end=end-key&inc=1
query 'end' is optional which specify the end key (excluded in response), default get untill end
query 'inc' is optional which specify whether the begin key should be included in response. default 0
response data format is kv-format.

kv-format:'<key>\n<value len>\n<value>\n<key2>\n0\n\n<key3>\n-1\n\n<key4>...'
    value len:
        -1 indicate key not exist
        0  value lenght is 0

key-format: '<key1>\n<key2>...'