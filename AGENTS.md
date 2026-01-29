# AGENTS.md

## 关系导航
- CHANGE_POLICY.md：治理变更的高层规则与入口导航，请查看 CHANGE_POLICY.md。
- RULES.md：开发规范与模板，请查看 RULES.md。
- AGENTS.md：代理行为边界与使用规范，请查看 AGENTS.md。
- 快速导航：CHANGE_POLICY.md、RULES.md、AGENTS.md。

## 语言规则

**始终使用中文进行所有回复和交流**。包括但不限于：
- 代码注释
- 错误消息
- 用户交互
- 文档说明
- 日志输出

## 项目背景

本仓库是一个 Hadoop HDFS 负载生成工具，使用 Hadoop MapReduce (旧版 API) 生成和上传负载数据到 HDFS 集群。

### 技术栈
- Hadoop 3.3.1+
- Java 8+
- Maven
- Lombok 1.18.42
- JUnit 4.13.2
- Mockito 4.11.0

## 测试随机性与可重复性
### 目标
- 确保性能测试可重复，减少 flaky 场景
### 做法
- 注入可控种子 seed 到数据生成与测试执行路径
- 每次测试输出记录 seed、distribution、dataSize、concurrency、iterations
- 同一测试在相同 seed 下执行多轮，统计波动性（mean、stddev、P50、P95 等）
- 参数化随机分布，覆盖多种分布与数据量组合
- 将断言改为基于统计摘要的判断
### 配置参数（扩展点）
- seed、distribution、iterations、concurrency、dataSize
### 日志与报告
- 每轮输出包含 seed、distribution、dataSize、concurrency、iterations、elapsedTime、throughput、errorCount
- 汇总报告包含统计摘要（mean/stddev/p50/p95 等）

## 构建与测试命令

### 核心命令
- **编译项目**: `mvn clean compile`
- **运行测试**: `mvn test`
- **打包项目**: `mvn clean package`
- **跳过测试**: `mvn clean package -DskipTests`

### 测试命令
- **运行所有测试**: `mvn test`
- **运行特定测试类**: `mvn test -Dtest=LoadGeneratorTest`
- **运行特定测试方法**: `mvn test -Dtest=LoadGeneratorTest#testGenerateData`
- **跳过测试**: `mvn clean package -DskipTests`

## 代码风格指南

### 命名规范
- **类名**: PascalCase (如 `LoadGenerator`, `DataGenerator`)
- **方法名**: camelCase (如 `generateData`, `setupFileSystem`)
- **变量名**: camelCase (如 `dataPath`, `fileSystem`)
- **常量名**: UPPER_SNAKE_CASE (如 `HDFS_PORT`, `DEFAULT_SIZE`)
- **私有字段**: camelCase，无匈牙利命名法

### 导入与包结构
- 包名: `com.hadoop.test.loadgenerator`
- 导入顺序: 标准Java库 → 第三方库 → 项目内部类
- 避免使用通配符导入，优先使用显式导入
- Lombok 注解与其他注解分组

### 类结构
1. 包声明
2. 导入语句
3. 类级 Javadoc
4. 类声明
5. 静态字段 (常量)
6. 实例字段
7. 构造函数
8. 静态方法
9. 实例方法 (public → protected → private)
10. 内部类/嵌套类

### 格式规范
- 缩进: 4空格 (不使用Tab)
- 行长度: 建议不超过120字符
- 大括号: K&R风格 (左大括号在同一行)
- 空行: 方法间和逻辑组间单个空行，无尾随空格

### Lombok 使用
- 使用 `@Getter` 为需要getter的方法生成代码
- 仅在必要时使用 `@Setter`
- 复杂逻辑优先使用显式方法而非Lombok快捷方式
- 注解放置在字段或类级别合适位置

### 错误处理与日志
- 使用特定异常类型，避免通用 `Exception`
- 使用 SLF4J 记录错误: `LOG.error("描述性错误消息", exception)`
- 适当时候将检查异常包装为 RuntimeException
- 使用 `IllegalArgumentException` 处理无效参数
- 包含有意义的错误消息和上下文
- 使用 SLF4J: `private static final Logger LOG = LoggerFactory.getLogger(ClassName.class)`
- 日志级别: ERROR(错误), WARN(警告), INFO(重要事件), DEBUG(调试)
- 日志消息包含上下文，避免在热路径中使用字符串拼接

### Hadoop 特定模式
- 主应用实现 `Tool` 接口，使用 `ToolRunner.run()` 作为入口点
- 配置对象应传递而非静态存储
- Hadoop 的 `Text` 类用于键值而非 `String`
- 遵循 Hadoop 序列化模式
- 内存配置直接设置为 Hadoop 配置

### 注释与文档
- 公共类和方法使用 Javadoc，包含 `@param`, `@return`, `@throws`
- 有限使用行内注释，优先使用自文档化代码
- 文档化复杂业务逻辑或非直观实现

## 项目结构

```
src/main/java/com/hadoop/test/loadgenerator/
├── DataGenerator.java    # 生成测试数据
├── StructureGenerator.java # 生成目录结构
├── LoadGenerator.java    # 主入口，实现 Tool 接口
└── LoadGeneratorMR.java  # MapReduce 实现
```

## 开发注意事项

- 使用 Hadoop MapReduce 旧版 API (`mapred` 包)
- Lombok 通过 `@Getter` 减少样板代码
- 使用 DummyInputFormat 无需真实输入数据
- 配置选项应有合理的默认值
- 在适当级别记录日志，避免过多 DEBUG 日志
- 测试结果使用表格格式，使用 `String.format` 对齐列
- 测试中使用 `verify()`, `any()`, `argThat()` 等 matcher
