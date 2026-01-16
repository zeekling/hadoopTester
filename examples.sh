#!/bin/bash

# Hadoop HDFS RPC 压测工具 - 使用示例脚本

# 示例 1: 显示帮助信息
echo "=== 示例 1: 显示帮助信息 ==="
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" -Dexec.args="--help"

echo ""
echo "=== 示例 2: 测试所有操作类型（小规模） ==="
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 2 \
               --reduces 1 \
               --fileSize 1 \
               --opsPerMapper 10 \
               --operations mkdir,write,read,delete_dir,delete_file,ls"

echo ""
echo "=== 示例 3: 仅测试写入操作 ==="
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 3 \
               --reduces 1 \
               --fileSize 5 \
               --opsPerMapper 50 \
               --operations write"

echo ""
echo "=== 示例 4: 测试读写操作 ==="
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 5 \
               --reduces 2 \
               --fileSize 10 \
               --opsPerMapper 100 \
               --operations read,write"

echo ""
echo "=== 示例 5: 大规模压测 ==="
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 10 \
               --reduces 2 \
               --fileSize 50 \
               --opsPerMapper 1000 \
               --operations mkdir,write,read,delete_dir,delete_file,ls"
