#!/usr/bin/env bash
# Starts one daemon in the foreground, chosen by the first argument; anything else is exec'd as-is.
set -euo pipefail

case "${1:-}" in
  namenode)
    if [ ! -d /data/nn/current ]; then
      hdfs namenode -format -nonInteractive -force -clusterId pulse
    fi
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  historyserver)   exec mapred historyserver ;;
  # Roles below exist only in the bda/stack image
  hive-metastore)
    schematool -dbType postgres -info >/dev/null 2>&1 || schematool -dbType postgres -initSchema
    exec hive --service metastore ;;
  hiveserver2)     exec hive --service hiveserver2 ;;
  hbase-master)    exec hbase master start ;;
  hbase-regionserver) exec hbase regionserver start ;;
  *)               exec "$@" ;;
esac
