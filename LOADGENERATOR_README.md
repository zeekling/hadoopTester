# LoadGenerator 使用指南

本指南介绍了如何使用从 Apache Hadoop 迁移的 LoadGenerator 工具进行 HDFS NameNode 负载测试。

## 工具概述

LoadGenerator 是一个用于测试 HDFS NameNode 在不同客户端负载下行为的工具。它可以生成不同类型的操作（读、写、列表）来模拟实际的工作负载。

## 包含的工具

### 1. StructureGenerator
生成随机的目录结构和文件结构。

**命令格式:**
```bash
java com.hadoop.test.loadgenerator.StructureGenerator \
  -maxDepth <最大深度> \
  -minWidth <最小子目录数> \
  -maxWidth <最大子目录数> \
  -numOfFiles <文件数量> \
  -avgFileSize <平均文件大小(块)> \
  -outDir <输出目录> \
  -seed <随机数种子>
```

**示例:**
```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.StructureGenerator" \
  -Dexec.args="-maxDepth 5 -minWidth 1 -maxWidth 5 -numOfFiles 10 -avgFileSize 1 -outDir ."
```

**参数说明:**
- `-maxDepth`: 目录树的最大深度，默认 5
- `-minWidth`: 每个目录的最小子目录数，默认 1
- `-maxWidth`: 每个目录的最大子目录数，默认 5
- `-numOfFiles`: 总文件数量，默认 10
- `-avgFileSize`: 平均文件大小（以块为单位），默认 1
- `-outDir`: 输出目录，默认当前目录
- `-seed`: 随机数生成器种子

**输出文件:**
- `dirStructure`: 包含所有目录名称
- `fileStructure`: 包含所有文件名称及其大小

### 2. DataGenerator
根据 StructureGenerator 生成的结构文件在 HDFS 上创建实际的目录和文件。

**命令格式:**
```bash
java com.hadoop.test.loadgenerator.DataGenerator \
  -inDir <输入目录> \
  -root <HDFS根目录>
```

**示例:**
```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.DataGenerator" \
  -Dexec.args="-inDir . -root /testLoadSpace"
```

**参数说明:**
- `-inDir`: 包含目录/文件结构文件的输入目录，默认当前目录
- `-root`: 在 HDFS 上创建命名空间的根目录，默认 `/testLoadSpace`

### 3. LoadGenerator
多线程负载生成器，用于测试 NameNode 性能。

**命令格式:**
```bash
java com.hadoop.test.loadgenerator.LoadGenerator \
  -readProbability <读概率> \
  -writeProbability <写概率> \
  -root <测试空间根目录> \
  -maxDelayBetweenOps <操作间最大延迟(毫秒)> \
  -numOfThreads <线程数> \
  -elapsedTime <运行时间(秒)> \
  -startTime <开始时间(毫秒)> \
  -scriptFile <脚本文件> \
  -flagFile <标志文件>
```

**示例:**
```bash
# 使用固定概率运行 60 秒
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGenerator" \
  -Dexec.args="-readProbability 0.3333 -writeProbability 0.3333 -root /testLoadSpace -numOfThreads 200 -elapsedTime 60"

# 使用脚本文件
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGenerator" \
  -Dexec.args="-scriptFile script.txt -root /testLoadSpace -numOfThreads 200"
```

**参数说明:**
- `-readProbability`: 读操作概率 [0, 1]，默认 0.3333
- `-writeProbability`: 写操作概率 [0, 1]，默认 0.3333
- `-root`: 测试空间根目录，默认 `/testLoadSpace`
- `-maxDelayBetweenOps`: 操作间的最大延迟（毫秒），默认 0（无延迟）
- `-numOfThreads`: 生成的线程数，默认 200
- `-elapsedTime`: 程序运行时间（秒），默认 0（无限运行）
- `-startTime`: 线程开始运行的时间（毫秒）
- `-scriptFile`: 包含脚本操作模式的文本文件
- `-flagFile`: 用于提前停止测试的标志文件，默认 `/tmp/flagFile`

**脚本文件格式:**
脚本文件每行包含三个值，用空格分隔：
```
<持续时间(秒)> <读概率> <写概率>
```

示例脚本文件 `script.txt`:
```
60 0.3333 0.3333
120 0.5 0.25
60 0.25 0.5
```

**测试操作类型:**
- `open`: 打开/读取文件
- `list`: 列出目录内容
- `create`: 创建文件
- `write_close`: 写入并关闭文件
- `delete`: 删除文件

### 4. LoadGeneratorMR
MapReduce 版本的负载生成器，可以在分布式环境中运行。

**命令格式:**
```bash
java com.hadoop.test.loadgenerator.LoadGeneratorMR \
  -mr <Map任务数> <输出目录> \
  <其他 LoadGenerator 参数>
```

**示例:**
```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGeneratorMR" \
  -Dexec.args="-mr 5 /loadgen/output -readProbability 0.3333 -writeProbability 0.3333 -root /testLoadSpace -numOfThreads 50 -elapsedTime 60"
```

**参数说明:**
- `-mr`: MapReduce 模式参数，必须作为前三个参数
  - 第一个参数: Map 任务数量
  - 第二个参数: MR 输出目录
- 其他参数与 LoadGenerator 相同

**注意:** 作为 MapReduce 作业运行时，必须指定 `-elapsedTime` 或 `-scriptFile`。

## 典型使用流程

### 1. 准备测试环境
```bash
# 步骤 1: 生成目录和文件结构
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.StructureGenerator" \
  -Dexec.args="-maxDepth 5 -minWidth 2 -maxWidth 5 -numOfFiles 100 -avgFileSize 2 -outDir ./structure"

# 步骤 2: 在 HDFS 上创建测试数据
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.DataGenerator" \
  -Dexec.args="-inDir ./structure -root /testLoadSpace"

# 步骤 3: 运行负载测试
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGenerator" \
  -Dexec.args="-readProbability 0.4 -writeProbability 0.4 -root /testLoadSpace -numOfThreads 100 -elapsedTime 120"
```

### 2. 使用 MapReduce 模式
```bash
# 在分布式集群上运行负载测试
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGeneratorMR" \
  -Dexec.args="-mr 10 /loadgen/output -readProbability 0.3333 -writeProbability 0.3333 -root /testLoadSpace -numOfThreads 50 -elapsedTime 300"
```

### 3. 使用脚本文件测试不同负载
```bash
# 创建脚本文件 cat > load_script.txt << EOF
60 0.2 0.2
120 0.4 0.4
60 0.6 0.2
EOF

# 运行测试
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGenerator" \
  -Dexec.args="-scriptFile load_script.txt -root /testLoadSpace -numOfThreads 200"
```

### 4. 提前停止测试
```bash
# 在另一个终端中创建标志文件以提前停止测试
hdfs dfs -touchz /tmp/flagFile
```

## 输出结果

LoadGenerator 运行后会输出以下统计信息：

```
Result of running LoadGenerator against fileSystem: hdfs://namenode:8020
Average open execution time: 2.5ms
Average list execution time: 1.8ms
Average deletion execution time: 3.2ms
Average create execution time: 4.1ms
Average write_close execution time: 5.3ms
Average operations per second: 245.5ops/s
```

## 注意事项

1. **测试空间要求**: 在运行 LoadGenerator 之前，确保 HDFS 上有足够的测试空间，并且包含一些文件和目录
2. **资源限制**: 根据集群大小调整 `-numOfThreads` 参数，避免过度负载
3. **网络带宽**: 大量并发操作可能会消耗大量网络带宽
4. **监控**: 建议同时监控 NameNode 的指标以全面了解性能
5. **清理**: 测试完成后，记得清理测试数据（例如删除 `/testLoadSpace` 目录）

## 故障排除

### 问题: "The test space /testLoadSpace is empty"
**解决**: 先运行 StructureGenerator 和 DataGenerator 创建测试数据

### 问题: "Cannot specify both ElapsedTime and ScriptFile"
**解决**: 只能指定其中一个参数，不能同时使用

### 问题: MapReduce 作业失败
**解决**: 确保指定了 `-elapsedTime` 或 `-scriptFile`，并且输出目录不存在

## 与现有 HDFSRpcTest 的关系

LoadGenerator 和 HDFSRpcTest 都是 HDFS 性能测试工具，但它们有不同用途：

- **LoadGenerator**: 专注于测试 NameNode 的元数据操作性能（打开、列表、删除等）
- **HDFSRpcTest**: 测试更广泛的 HDFS 操作，包括读写文件、设置权限等，使用 MapReduce 框架

可以根据测试需求选择合适的工具，或者结合使用以获得全面的性能评估。
