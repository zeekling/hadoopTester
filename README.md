# Hadoop HDFS RPC 压测工具

这是一个用于测试 Hadoop HDFS RPC 性能的工具，通过 MapReduce 作业执行多种 HDFS 操作。

## 功能特性

- 支持 10 种 HDFS 操作类型：
  - `write` - 写入文件
  - `read` - 读取文件
  - `delete_file` - 删除文件
  - `rename` - 重命名文件
  - `get_file_status` - 获取文件状态
  - `exists` - 检查文件是否存在
  - `set_permission` - 设置文件权限
  - `append` - 追加数据到文件
  - `create_symlink` - 创建符号链接
  - `append_truncate` - 对同一文件频繁执行 append 和 truncate 操作

- 可配置参数：
  - Map 任务数量
  - Reduce 任务数量
  - Map 任务内存（MB）
  - Reduce 任务内存（MB）
  - 基础测试目录
  - 文件大小（MB）
  - 每个 Mapper 的操作次数
  - 操作类型列表
  - 异步线程池大小

- 结果统计：
  - 通过 Reducer 汇总统计结果
  - 输出操作次数、总耗时、平均耗时、最小/最大耗时
  - 统计错误次数

## 编译

```bash
mvn clean package
```

## 运行测试

### 基本用法

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
   -Dexec.args="--baseDir /test/hdfsrpc \
                 --maps 10 \
                 --reduces 1 \
                 --mapMemoryMb 1024 \
                 --reduceMemoryMb 512 \
                 --fileSize 10 \
                 --opsPerMapper 1000 \
                 --threadPoolSize 10 \
                 --operations write,read,delete_file,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate"
```

### 参数说明

| 参数              | 说明                        | 默认值 |
|--------------------|------------------------------|----------|
| `--maps`           | Map 任务数量               | 10       |
| `--reduces`        | Reduce 任务数量             | 1        |
| `--mapMemoryMb`    | Map 任务内存（MB）        | 1024     |
| `--reduceMemoryMb`  | Reduce 任务内存（MB）       | 512      |
| `--baseDir`         | 基础测试目录              | /test/hdfsrpc |
| `--operations`       | 操作类型（逗号分隔）        | 见下方完整列表 |
| `--fileSize`        | 文件大小（MB）             | 10       |
| `--opsPerMapper`    | 每个 Mapper 的操作次数       | 10000     |
| `--fileCount`        | 每个操作的文件数量         | 100       |
| `--threadPoolSize`    | 异步操作线程池大小          | 10       |
| `--help`            | 显示帮助信息              | -         |

**`--operations` 参数的完整默认值为：**
```
write,read,delete_file,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate
```

### 操作类型详解

| 操作类型         | 说明                                     |
|-----------------|------------------------------------------|
| `write`         | 写入指定大小的随机数据到文件              |
| `read`          | 读取之前写入的文件内容                   |
| `delete_file`   | 删除指定的文件                            |
| `rename`         | 重命名文件                                |
| `get_file_status`| 获取文件的元数据状态                    |
| `exists`         | 检查文件是否存在                        |
| `set_permission` | 设置文件的权限（默认 0644）                |
| `append`         | 追加 1KB 数据到已有文件                 |
| `create_symlink`  | 为文件创建符号链接                       |
| `append_truncate` | 对同一文件循环执行 append 和 truncate 操作（10次） |

### 使用示例

#### 测试所有操作类型

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" \
  -Dexec.args="--baseDir /test/hdfsrpc \
                --maps 20 \
                --reduces 2 \
                --fileSize 50 \
                --opsPerMapper 5000 \
                --operations write,read,delete_file,rename,get_file_status,exists"
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

## 测试

### 运行所有测试

```bash
mvn test
```

### 运行特定测试类

```bash
mvn test -Dtest=HdfsOperationTest
```

### 运行特定测试方法

```bash
mvn test -Dtest=HdfsOperationTest#testExecuteWrite
```

## 输出结果

测试结果由 Reducer 汇总统计，每行输出一个操作类型的统计数据。

### 输出格式

```
Operation=<操作名>, Count=<总次数>, errorCount=<错误次数>, totalTime=<总耗时ms>, avgTime=<平均耗时ms>, minTime=<最小耗时ms>, maxTime=<最大耗时ms>
```

### 输出示例

```
Operation=write, Count=10000, errorCount=5, totalTime=120000, avgTime=12, minTime=5, maxTime=100
Operation=read, Count=10000, errorCount=2, totalTime=80000, avgTime=8, minTime=3, maxTime=80
Operation=delete_file, Count=10000, errorCount=1, totalTime=4000, avgTime=0, minTime=0, maxTime=40
Operation=rename, Count=10000, errorCount=0, totalTime=5000, avgTime=0, minTime=1, maxTime=50
```

### 统计指标说明

- **Operation**: 操作类型（write, read, delete_file, rename, get_file_status, exists 等）
- **Count**: 该操作执行的总次数
- **errorCount**: 该操作执行失败的次数
- **totalTime(ms)**: 所有成功操作的总耗时（毫秒）
- **avgTime(ms)**: 平均每次成功操作的耗时（毫秒）
- **minTime(ms)**: 最快的一次成功操作耗时（毫秒）
- **maxTime(ms)**: 最慢的一次成功操作耗时（毫秒）

## 项目结构

```
src/main/java/com/hadoop/test/
 ├── ArgumentParser.java      # CLI 参数解析
 ├── HDFSRpcTest.java        # 主入口，实现 Tool 接口
 ├── ConfigOption.java       # 配置选项及默认值
 ├── Constants.java          # 应用常量
 ├── HdfsOperation.java      # HDFS 操作实现（10 种操作）
 ├── SliveMapper.java        # Hadoop Mapper 实现
 ├── SliveReducer.java       # Hadoop Reducer 实现
 ├── SlivePartitioner.java   # 自定义分区器
 ├── DummyInputFormat.java   # 测试用输入格式
 └── OperationOutput.java    # 输出数据结构
```

## 技术架构

### 核心组件

- **HdfsOperation**: 封装所有 HDFS 操作，支持同步和异步执行
- **SliveMapper**: 执行配置的操作，每个 mapper 处理一个 dummy input
- **SliveReducer**: 聚合同一操作类型的所有结果，计算统计数据
- **ExecutorService**: 异步执行操作，提高并发性能

### 执行流程

1. **Mapper 阶段**：
   - 接收 dummy input
   - 根据配置循环执行指定的操作
   - 使用线程池异步执行操作
   - 输出每个操作的结果（类型:时长）
   - 最后输出 total_errors 计数

2. **Reducer 阶段**：
   - 接收同一操作类型的所有结果
   - 计算总次数、错误次数、总耗时
   - 计算平均、最小、最大耗时
   - 输出统计信息

## 系统要求

- Java 17+
- Hadoop 3.4.2
- Maven 3.6+

## 注意事项

1. 确保测试目录（`--baseDir`）存在且有写权限
2. 根据集群资源调整 Map 和 Reduce 任务数量
3. 大文件操作需要足够的磁盘空间和内存
4. 建议先在测试环境运行，确认无误后再在生产环境使用
5. `append_truncate` 操作会频繁打开/关闭文件，性能影响较大
6. 线程池大小（`--threadPoolSize`）应根据集群资源合理设置

## 许可证

Apache License 2.0

## 变更概览：LoadGeneratorMR 拆分为顶层实现

- 本仓库的 LoadGeneratorMR 内部实现已拆分为顶层类，避免嵌套内部类，相关实现位于以下路径：
  - src/main/java/com/hadoop/test/loadgenerator/EmptySplit.java
  - src/main/java/com/hadoop/test/loadgenerator/DummyInputFormat.java
  - src/main/java/com/hadoop/test/loadgenerator/DummySingleRecordReader.java
  - src/main/java/com/hadoop/test/loadgenerator/ProgressThread.java
  - src/main/java/com/hadoop/test/loadgenerator/MapperThatRunsNNLoadGenerator.java
  - src/main/java/com/hadoop/test/loadgenerator/ReducerThatCollectsLGdata.java
  - src/main/java/com/hadoop/test/loadgenerator/LGConstants.java
- LoadGeneratorMR 现在引用上述顶层实现类来运行 MR 任务，避免将逻辑保留在单一内部类中，便于测试和扩展。
- 运行 MR 的示例：
  - mvn clean package
  - mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGeneratorMR" -Dexec.args="-mr 2 /path/to/output [其他参数]"
- 随机性相关配置（seed/distribution/dataSize/iterations/concurrency）通过 ArgumentParser/ConfigOption 管理，提升测试可重复性与覆盖性。

## 新增使用与测试指南（简要）
- 要运行 MR 作业，请确保 Hadoop 集群就绪，并传入必要的参数。
- 也可以通过 HDFSRpcTest 入口进行非 MR 的基线测试，具体参数保持与之前一致。
- AGENTS.md 中新增了“测试随机性与可重复性”分节，详述数据生成的 seed 注入、分布配置以及统计输出等要点。
