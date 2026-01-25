# AGENTS.md

## 语言规则

**始终使用中文进行所有回复和交流**。包括但不限于：
- 代码注释
- 错误消息
- 用户交互
- 文档说明
- 日志输出

## 项目背景介绍

本仓库是一个 Hadoop HDFS RPC 性能测试工具，旨在测试和验证 HDFS 集群的 RPC 操作性能。该工具通过模拟多种 HDFS 操作，评估集群的响应时间和吞吐量，帮助开发者和运维人员了解系统性能瓶颈。

### 主要目标
- 测试 HDFS 核心操作的 RPC 响应时间
- 支持并发测试，模拟真实负载场景
- 提供详细的性能指标和统计报告
- 兼容 Hadoop 3.4.2 版本

### 技术栈
- Hadoop 3.4.2
- Java 17+
- Maven
- Hadoop MapReduce (旧版 API)
- Lombok
- Mockito

## 贡献指南

### 如何贡献
1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

### 提交规范
- 使用有意义的提交信息
- 提交信息格式：`<type>(<scope>): <subject>`
- Type: feat, fix, docs, style, refactor, test, chore
- Scope: 指明影响的模块或功能

### 代码审查流程
1. 所有 PR 必须通过 CI 检查
2. 至少一位维护者审核通过
3. 确保代码符合编码规范
4. 所有测试必须通过

### 开发规范
- 遵循现有代码风格
- 添加必要的单元测试
- 更新相关文档
- 保持代码简洁和可维护性

## 架构设计

### 整体架构
```
┌─────────────────┐
│  CLI Argument   │
│  Parser         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  HDFSRpcTest    │ (Main Application)
│  - Job Setup    │
│  - Operation    │
│  - Result       │
└────────┬────────┘
         │
    ┌────┴────┬────────┐
    ▼         ▼        ▼
┌─────────┐ ┌────────┐ ┌──────────┐
│ Mapper  │ │Reducer │ │ HDFS     │
│         │ │        │ │ Operation│
└─────────┘ └────────┘ └──────────┘
```

### 核心模块

#### 1. ArgumentParser
负责解析命令行参数，提供类型安全的配置获取接口。

#### 2. HDFSRpcTest
主应用程序，实现 Tool 接口，负责：
- 配置 Hadoop Job
- 协调 Mapper 和 Reducer 执行
- 收集和汇总测试结果
- 输出格式化报告

#### 3. HdfsOperation
封装所有 HDFS 操作，支持同步和异步执行：
- 文件写入/读取
- 文件删除/重命名
- 权限设置
- 符号链接创建
- 文件存在性检查
- 文件状态获取
- 文件追加/截断

#### 4. MapReduce 组件
- **SliveMapper**: 每个操作作为 map 任务，计算单个操作耗时
- **SliveReducer**: 汇总所有 map 输出，生成最终统计结果
- **SlivePartitioner**: 自定义分区策略

### 数据流

```
1. 用户输入参数
   ↓
2. ArgumentParser 解析参数
   ↓
3. HDFSRpcTest 创建 HDFS 操作实例
   ↓
4. 操作分配给 Mapper 执行
   ↓
5. Mapper 记录操作耗时
   ↓
6. Reducer 汇总统计
   ↓
7. 输出格式化报告
```

### 测试设计
- 使用 DummyInputFormat 无需真实输入数据
- 单元测试使用 Mockito 模拟依赖
- 集成测试使用本地 HDFS
- 测试目录自动清理，避免副作用

## 编码指南

This file contains guidelines and commands for agentic coding agents working in this Hadoop testing repository.

## Build System & Commands

This is a Maven-based Java project. Use these commands for development:

### Core Commands
- **Build project**: `mvn clean compile`
- **Run tests**: `mvn test`
- **Package JAR**: `mvn clean package`
- **Run main class**: `mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest"`
- **Run with args**: `mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" -Dexec.args="--maps 5 --baseDir /test"`

### Test Commands
- **Run all tests**: `mvn test`
- **Run specific test class**: `mvn test -Dtest=ClassName`
- **Run specific test method**: `mvn test -Dtest=ClassName#methodName`
- **Skip tests**: `mvn clean package -DskipTests`

### Lint/Typecheck
No explicit lint plugins configured. Ensure all tests pass before committing. Use `@SuppressWarnings("resource")` on @Before/@After methods that delete test directories.

## Project Structure

```
src/main/java/com/hadoop/test/
├── ArgumentParser.java      # CLI argument parsing
├── HDFSRpcTest.java        # Main entry point, implements Tool
├── ConfigOption.java       # Configuration options with defaults
├── Constants.java          # Application constants
├── HdfsOperation.java      # HDFS operation implementations (10 ops)
├── SliveMapper.java        # Hadoop mapper implementation
├── SliveReducer.java       # Hadoop reducer implementation
├── SlivePartitioner.java   # Custom partitioner
├── DummyInputFormat.java   # Input format for testing
└── OperationOutput.java    # Output data structure
```

## Code Style Guidelines

### Package & Imports
- Package: `com.hadoop.test`
- Import order: Standard Java, third-party, project imports (grouped/sorted)
- Prefer explicit imports over wildcards
- Lombok annotations grouped with other annotations

### Naming Conventions
- **Classes**: PascalCase (e.g., `ArgumentParser`, `HDFSRpcTest`)
- **Methods**: camelCase (e.g., `parse()`, `getOutputPath()`)
- **Variables**: camelCase (e.g., `optList`, `parsedData`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `PROG_NAME`, `TYPE_SEP`)
- **Private fields**: camelCase, no Hungarian notation
- **Static final fields**: UPPER_SNAKE_CASE

### Class Structure
1. Package declaration, 2. Imports (grouped), 3. Class-level Javadoc, 4. Class declaration
5. Static fields (constants first), 6. Instance fields, 7. Constructors, 8. Static methods
9. Instance methods (public→protected→private), 10. Inner/nested classes

### Lombok Usage
- Use `@Getter` for fields needing getters
- Use `@Setter` only when necessary
- Prefer explicit methods for complex logic over Lombok shortcuts
- Place Lombok annotations at field or class level as appropriate

### Error Handling & Logging
- Use specific exception types, avoid generic `Exception`
- Log errors using SLF4J: `LOG.error("Error message", exception)`
- Wrap checked exceptions in RuntimeException when appropriate
- Use `IllegalArgumentException` for invalid parameters
- Include meaningful error messages with context
- Use SLF4J with `LoggerFactory.getLogger(ClassName.class)`, field name: `private static final Logger LOG`
- Log levels: ERROR for errors, WARN for warnings, INFO for important events, DEBUG for debugging
- Include context in log messages, avoid string concatenation in hot paths

### Hadoop-Specific Patterns
- Implement `Tool` interface for main applications, use `ToolRunner.run()` for entry point
- Configuration objects should be passed, not stored statically
- Use Hadoop's `Text` class instead of `String` for keys/values
- Follow Hadoop's serialization patterns for custom types
- Memory configs (mapMemoryMb, reduceMemoryMb) use String type set directly to Hadoop config

### Comments & Documentation
- Use Javadoc for public classes and methods with `@param`, `@return`, and `@throws` where applicable
- Use inline comments sparingly, prefer self-documenting code
- Document complex business logic or non-obvious implementations

### Code Formatting
- Indentation: 4 spaces (no tabs), line length: prefer under 120 characters
- Braces: K&R style (opening brace on same line)
- Spacing: Single blank line between methods and logical groups, no trailing whitespace

### Dependencies Management
- Hadoop 3.4.2, Java 17 (maven.compiler.release)
- Lombok 1.18.42, JUnit 4.13.2, Mockito 4.11.0

## Common Patterns

### Argument Parsing & Configuration
```java
ArgumentParser argHolder = new ArgumentParser(args);
ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
String value = parsedOpts.getValue(ConfigOption.OPTION_NAME.getOpt());
int intValue = parsedOpts.getValueAsInt(ConfigOption.INT_OPTION.getOpt(), defaultValue);
```

### Error Handling
```java
try {
    // operation
} catch (SpecificException e) {
    LOG.error("Descriptive error message", e);
    throw new RuntimeException("Wrapper message", e);
}
```

### Hadoop Job Setup
```java
JobConf job = new JobConf(base, MainClass.class);
job.setMapperClass(MapperClass.class);
job.setReducerClass(ReducerClass.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
```

### HDFS Operations
All operations in HdfsOperation follow this pattern:
- Method: `execute[OperationName](int index, long startTime)`
- Returns: `new OperationOutput(OutputType.LONG, "operation_name", "duration", duration, 1)`
- Operations: write, read, delete_file, rename, get_file_status, exists, set_permission, append, create_symlink, append_truncate

### Async Operations
```java
CompletableFuture<OperationOutput> future = operation.executeAsync("operation", index);
OperationOutput result = future.get(10, TimeUnit.SECONDS);
```

### Table Formatting
```java
String row = String.format("| %-10s | %4d | %10d |", "op", count, total);
```

### Mocking in Tests
```java
output = mock(OutputCollector.class);
verify(reporter).setStatus(contains("message"));
doAnswer(invocation -> {
    collectedKeys.add(invocation.getArgument(0));
    return null;
}).when(output).collect(any(), any(Text.class));
```

### Test Directory Setup
```java
Path testBaseDir = new Path("target/test-data/testname-" + System.currentTimeMillis());
FileSystem localFs = FileSystem.getLocal(new Configuration());
localFs.mkdirs(testBaseDir);
// test code
localFs.delete(testBaseDir, true); // in @After with @SuppressWarnings("resource")
```

## Development Notes

- Project uses Hadoop MapReduce (old API) with `mapred` package
- Lombok reduces boilerplate with `@Getter` annotations
- Use `DummyInputFormat` for testing without real input data
- Configuration options should have sensible defaults
- Log at appropriate levels, avoid excessive DEBUG logging
- Test results use table format with `String.format` for aligned columns
- Mockito is used with `verify()`, `any()`, `argThat()` matchers
- HDFS operations support 10 different operation types for comprehensive RPC testing
- SliveReducer outputs with null key - use `any()` instead of `any(Text.class)` in mocks
- SliveMapper outputs 1 extra collect call for "total_errors" at the end of each map
- Use `System.currentTimeMillis()` suffixes for unique test directory names
