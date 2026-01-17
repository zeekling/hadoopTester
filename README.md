# Hadoop HDFS RPC 压测工具

这是一个用于测试 Hadoop HDFS RPC 性能的工具，通过 MapReduce 作业执行多种 HDFS 操作。

## 功能特性

- 支持多种 HDFS 操作类型：
  - `mkdir` - 创建目录
  - `write` - 写入文件
  - `read` - 读取文件
  - `delete_dir` - 删除目录
  - `delete_file` - 删除文件
  - `ls` - 列出文件

- 可配置参数：
  - Map 任务数量
  - Reduce 任务数量
  - 基础测试目录
  - 文件大小（MB）
  - 每个 Mapper 的操作次数
  - 操作类型列表
  - 异步线程池大小

- 结果统计：
  - 通过 Reducer 汇总统计结果
  - 输出操作耗时等指标

## 编译

```bash
mvn clean package
```

## 使用方法

### 基本用法

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 10 \
               --reduces 1 \
               --fileSize 10 \
               --opsPerMapper 1000 \
               --threadPoolSize 10 \
               --operations mkdir,write,read,delete_dir,delete_file,ls"
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--maps` | Map 任务数量 | 10 |
| `--reduces` | Reduce 任务数量 | 1 |
| `--baseDir` | 基础测试目录 | /test/hdfsrpc |
| `--operations` | 操作类型（逗号分隔） | mkdir,write,read,delete_dir,delete_file,ls |
| `--fileSize` | 文件大小（MB） | 10 |
| `--opsPerMapper` | 每个 Mapper 的操作次数 | 1000 |
| `--fileCount` | 每个操作的文件数量 | 100 |
| `--dirCount` | 每个操作的目录数量 | 10 |
| `--threadPoolSize` | 异步操作线程池大小 | 10 |
| `--help` | 显示帮助信息 | - |

### 操作类型

- `mkdir` - 创建目录测试
- `write` - 写入文件测试
- `read` - 读取文件测试
- `delete_dir` - 删除目录测试
- `delete_file` - 删除文件测试
- `ls` - 列出文件测试

### 示例

#### 测试所有操作类型

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 20 \
               --reduces 2 \
               --fileSize 50 \
               --opsPerMapper 5000 \
               --operations mkdir,write,read,delete_dir,delete_file,ls"
```

#### 仅测试写入操作

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 10 \
               --reduces 1 \
               --fileSize 100 \
               --opsPerMapper 2000 \
               --operations write"
```

#### 测试读写操作

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
               --maps 5 \
               --reduces 1 \
               --fileSize 10 \
               --opsPerMapper 1000 \
               --operations read,write"
```

## 输出结果

测试结果由 Reducer 汇总统计，输出格式为：
```
<操作类型>    count=<操作次数>, total=<总耗时>ms, avg=<平均耗时>ms, min=<最小耗时>ms, max=<最大耗时>ms
```

### 输出示例
```
mkdir    count=10000, total=5000ms, avg=0ms, min=0ms, max=50ms
write    count=10000, total=120000ms, avg=12ms, min=5ms, max=100ms
read     count=10000, total=80000ms, avg=8ms, min=3ms, max=80ms
delete_dir    count=10000, total=6000ms, avg=0ms, min=0ms, max=60ms
delete_file    count=10000, total=4000ms, avg=0ms, min=0ms, max=40ms
ls    count=10000, total=30000ms, avg=3ms, min=1ms, max=30ms
```

### 统计指标说明
- **count**: 该操作执行的总次数
- **total**: 所有操作的总耗时（毫秒）
- **avg**: 平均每次操作的耗时（毫秒）
- **min**: 最快的一次操作耗时（毫秒）
- **max**: 最慢的一次操作耗时（毫秒）

## 系统要求

- Java 17+
- Hadoop 3.4.2
- Maven 3.6+

## 注意事项

1. 确保测试目录（`--baseDir`）存在且有写权限
2. 根据集群资源调整 Map 和 Reduce 任务数量
3. 大文件操作需要足够的磁盘空间和内存
4. 建议先在测试环境运行，确认无误后再在生产环境使用
