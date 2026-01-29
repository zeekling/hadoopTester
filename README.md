# Hadoop HDFS Load Tester

本仓库是一个 Hadoop HDFS 负载生成工具，使用 Hadoop MapReduce (旧版 API) 生成和上传负载数据到 HDFS 集群。

## 技术栈
- Hadoop 3.3.1+ 
- Java 8+ 
- Maven
- Lombok 1.18.42
- JUnit 4.13.2
- Mockito 4.11.0

## 主要组件
- DataGenerator.java：读取输入的目录结构，生成命名空间，在给定的 root 下创建结构和文件，文件内容填充 'a'。
- StructureGenerator.java：随机生成命名空间树和文件结构，输出 dirStructure 和 fileStructure 文件。
- LoadGenerator.java：主入口，实现 Hadoop Tool 接口，作为任务提交入口。
- LoadGeneratorMR.java：MapReduce 实现（旧版 API）。

## 构建与测试
- 构建: `mvn clean compile`
- 测试: `mvn test`
- 打包: `mvn clean package`
- 跳过测试: `mvn clean package -DskipTests`

## 快速开始
- 生成结构
  ```bash
  java -cp target/hadoopTester-1.0-SNAPSHOT.jar com.hadoop.test.loadgenerator.StructureGenerator \
    -maxDepth 3 -minWidth 1 -maxWidth 3 -numOfFiles 4 -avgFileSize 1 -outDir structureOut -seed 123
  ```
- 生成数据
  ```bash
  java -cp target/hadoopTester-1.0-SNAPSHOT.jar com.hadoop.test.loadgenerator.DataGenerator \
    -inDir structureOut -root /testLoadSpace
  ```

输出
- dirStructure：输出到 outDir/dirStructure
- fileStructure：输出到 outDir/fileStructure

默认值
- StructureGenerator: maxDepth=5, minWidth=1, maxWidth=5, numOfFiles=10, avgFileSize=1, outDir=当前目录, seed=当前时间
- DataGenerator: inDir=StructureGenerator.DEFAULT_STRUCTURE_DIRECTORY, root=DEFAULT_ROOT (/testLoadSpace)

## 许可证
- 代码头部使用 Apache 2.0 许可证，请遵守相关条款。

## 参考文档
- CHANGE_POLICY.md
- RULES.md
- AGENTS.md

如需，我可以将这份 README 翻译为英文版，或者添加更详细的示例和截图。
