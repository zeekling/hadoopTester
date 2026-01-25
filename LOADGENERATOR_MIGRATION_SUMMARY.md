# LoadGenerator 迁移总结

## 迁移完成

已成功将 Apache Hadoop 仓库中的 LoadGenerator 逻辑迁移到当前项目。

## 迁移的文件

### 1. StructureGenerator.java
- **路径**: `src/main/java/com/hadoop/test/loadgenerator/StructureGenerator.java`
- **功能**: 生成随机的目录结构和文件结构
- **用途**: 为 LoadGenerator 创建测试数据结构

### 2. DataGenerator.java
- **路径**: `src/main/java/com/hadoop/test/loadgenerator/DataGenerator.java`
- **功能**: 根据结构文件在 HDFS 上创建实际的目录和文件
- **用途**: 将 StructureGenerator 生成的结构应用到实际文件系统

### 3. LoadGenerator.java
- **路径**: `src/main/java/com/hadoop/test/loadgenerator/LoadGenerator.java`
- **功能**: 多线程负载生成器，测试 HDFS NameNode 性能
- **用途**: 生成读、写、列表等 HDFS 操作，测试 NameNode 的元数据操作性能

### 4. LoadGeneratorMR.java
- **路径**: `src/main/java/com/hadoop/test/loadgenerator/LoadGeneratorMR.java`
- **功能**: MapReduce 版本的负载生成器
- **用途**: 在分布式集群环境中运行负载测试

## 适配的更改

### 1. 包名修改
- 从 `org.apache.hadoop.fs.loadGenerator` 改为 `com.hadoop.test.loadgenerator`

### 2. 移除不兼容的类
- 将 `SubjectInheritingThread` 替换为标准的 `Thread` 类
- 这是因为 `SubjectInheritingThread` 在 Hadoop 3.4.2 中不存在

### 3. 添加中文注释和文档
- 保持原有的代码结构和功能
- 添加了详细的中文使用指南（LOADGENERATOR_README.md）

## 测试验证

### 单元测试
- **文件**: `src/test/java/com/hadoop/test/loadgenerator/TestLoadGenerator.java`
- **测试结果**: ✅ 6 个测试全部通过
  - StructureGenerator 基本功能测试
  - LoadGenerator 参数解析测试
  - LoadGenerator 错误处理测试
  - LoadGeneratorMR 参数验证测试

### 编译状态
- **状态**: ✅ 编译成功，无错误
- **警告**: 与原项目相同的 PosixParser 废弃警告（不影响功能）

## 使用方式

### 基本使用流程

```bash
# 1. 生成目录和文件结构
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.StructureGenerator" \
  -Dexec.args="-maxDepth 5 -minWidth 2 -maxWidth 5 -numOfFiles 100 -avgFileSize 2 -outDir ./structure"

# 2. 在 HDFS 上创建测试数据
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.DataGenerator" \
  -Dexec.args="-inDir ./structure -root /testLoadSpace"

# 3. 运行负载测试
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGenerator" \
  -Dexec.args="-readProbability 0.4 -writeProbability 0.4 -root /testLoadSpace -numOfThreads 100 -elapsedTime 120"
```

### MapReduce 模式

```bash
mvn exec:java -Dexec.mainClass="com.hadoop.test.loadgenerator.LoadGeneratorMR" \
  -Dexec.args="-mr 10 /loadgen/output -readProbability 0.3333 -writeProbability 0.3333 -root /testLoadSpace -numOfThreads 50 -elapsedTime 300"
```

详细使用说明请参考 `LOADGENERATOR_README.md` 文档。

## 项目结构

```
hadoopTester/
├── src/
│   ├── main/java/com/hadoop/test/
│   │   ├── loadgenerator/
│   │   │   ├── StructureGenerator.java      # 新增
│   │   │   ├── DataGenerator.java           # 新增
│   │   │   ├── LoadGenerator.java          # 新增
│   │   │   └── LoadGeneratorMR.java       # 新增
│   │   ├── HDFSRpcTest.java               # 原有
│   │   ├── ArgumentParser.java               # 原有
│   │   ├── ConfigOption.java                # 原有
│   │   ├── Constants.java                  # 原有
│   │   ├── HdfsOperation.java              # 原有
│   │   ├── SliveMapper.java                # 原有
│   │   ├── SliveReducer.java               # 原有
│   │   ├── SlivePartitioner.java           # 原有
│   │   ├── DummyInputFormat.java            # 原有
│   │   └── OperationOutput.java             # 原有
│   └── test/java/com/hadoop/test/
│       ├── loadgenerator/
│       │   └── TestLoadGenerator.java     # 新增
│       └── ...
├── LOADGENERATOR_README.md                 # 新增：使用指南
├── RULES.md                              # 新增：全局规则
└── pom.xml
```

## 功能特性

### LoadGenerator 核心功能

1. **多种操作类型**
   - 打开/读取文件（open）
   - 列出目录内容（list）
   - 创建文件（create）
   - 写入并关闭文件（write_close）
   - 删除文件（delete）

2. **灵活的负载配置**
   - 可配置读、写、列表操作的概率
   - 可配置线程数量
   - 可配置操作间延迟
   - 可配置运行时长

3. **脚本支持**
   - 支持使用脚本文件定义不同阶段的负载模式
   - 脚本格式：`<持续时间> <读概率> <写概率>`

4. **性能统计**
   - 各类操作的平均执行时间
   - 操作吞吐量
   - 详细的性能报告

5. **运行模式**
   - 单机多线程模式
   - MapReduce 分布式模式

## 与现有工具的对比

| 特性 | LoadGenerator | HDFSRpcTest |
|------|--------------|--------------|
| 主要测试目标 | NameNode 元数据操作 | 广泛的 HDFS RPC 操作 |
| 支持操作类型 | 5 种（open, list, create, write_close, delete） | 10 种（write, read, delete, rename, get_file_status, exists, set_permission, append, create_symlink, append_truncate） |
| 运行模式 | 单机多线程 + MapReduce | MapReduce |
| 配置灵活性 | 高（脚本支持） | 中（参数配置） |
| 测试数据准备 | 需要 StructureGenerator + DataGenerator | 不需要 |

## 依赖关系

LoadGenerator 的所有依赖都已在项目中配置：

- `hadoop-hdfs` 3.4.2 ✅
- `hadoop-common` 3.4.2 ✅
- `hadoop-mapreduce-client-jobclient` 3.4.2 ✅
- 其他 Hadoop 依赖 ✅

无需添加额外的 Maven 依赖。

## 验证清单

- ✅ 所有源代码文件成功迁移
- ✅ 包名正确修改为 `com.hadoop.test.loadgenerator`
- ✅ 移除了不兼容的 `SubjectInheritingThread`
- ✅ 项目编译成功
- ✅ 单元测试全部通过（6/6）
- ✅ 创建了详细的使用指南
- ✅ 保持了原有的功能和接口

## 后续建议

1. **集成测试**: 在真实的 HDFS 集群环境中进行端到端测试
2. **性能优化**: 根据实际使用场景优化参数配置
3. **文档完善**: 根据实际使用反馈补充使用案例
4. **监控集成**: 将 LoadGenerator 与集群监控工具集成

## 注意事项

1. LoadGenerator 需要在 HDFS 集群或本地 HDFS 环境中运行
2. 使用前需要先运行 StructureGenerator 和 DataGenerator 创建测试数据
3. 根据集群规模调整线程数，避免过度负载
4. Windows 环境需要注意 HADOOP_HOME 环境变量设置

---

**迁移完成时间**: 2026-01-24
**迁移状态**: ✅ 成功完成
**编译状态**: ✅ 通过
**测试状态**: ✅ 通过
