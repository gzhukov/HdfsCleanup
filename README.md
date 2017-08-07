# HdfsCleanup
Small java hdfs cleanup implementation

Usage:
java -jar hdfs-cleanup.jar -p /tmp/ -ts 3600
Params:
"-path", "-p" - "HDFS path"
"-ts" - "Cleanup before date.now() - ts", default: 86400; // 24h
"-recursive", "-r" - "Recursive delete" - recursively delete all paths in target path. Faster but unsafty.
