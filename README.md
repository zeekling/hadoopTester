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
    - `rename` - 重命名文件
    - `get_file_status` - 获取文件状态
    - `exists` - 检查文件是否存在
    - `set_permission` - 设置文件权限
    - `append` - 追加数据到文件
    - `create_symlink` - 创建符号链接
    - `append_truncate` - 对同一文件频繁执行append和truncate操作

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
                --mapMemoryMb 1024 \
                --reduceMemoryMb 512 \
                --fileSize 10 \
                --opsPerMapper 1000 \
                --threadPoolSize 10 \
                 --operations mkdir,write,read,delete_dir,delete_file,ls,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate"
 ```

 ### 参数说明

 | 参数              | 说明                        | 默认值 |
 |--------------------|------------------------------|----------|
 | `--maps`           | Map 任务数量               | 10       |
 | `--reduces`        | Reduce 任务数量             | 1        |
 | `--mapMemoryMb`    | Map 任务内存（MB）        | 1024     |
 | `--reduceMemoryMb`  | Reduce 任务内存（MB）       | 512      |
 | `--baseDir`         | 基础测试目录              | /test/hdfsrpc |
 | `--operations`       | 操作类型（逗号分隔）        | mkdir,write,read,delete_dir... |
 | `--fileSize`        | 文件大小（MB）             | 10        |
 | `--opsPerMapper`    | 每个 Mapper 的操作次数       | 1000      |
 | `--fileCount`        | 每个操作的文件数量         | 100       |
 | `--dirCount`         | 每个操作的目录数量         | 10       |
  | `--threadPoolSize`    | 异步操作线程池大小          | 10       |
  | `--help`            | 显示帮助信息              | -         |

**注**：`--operations` 参数的完整默认值为：
`mkdir,write,read,delete_dir,delete_file,ls,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate`

 ### 操作类型

- `mkdir` - 创建目录测试
- `write` - 写入文件测试
- `read` - 读取文件测试
- `delete_dir` - 删除目录测试
- `delete_file` - 删除文件测试
- `ls` - 列出文件测试
- `rename` - 重命名文件测试
- `get_file_status` - 获取文件状态测试
- `exists` - 检查文件是否存在测试
- `set_permission` - 设置文件权限测试
- `append` - 追加数据到文件测试
- `create_symlink` - 创建符号链接测试
- `append_truncate` - 对同一文件频繁执行append和truncate操作测试

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

测试结果由 Reducer 汇总统计，以表格格式输出。

### 输出示例

```
========== Test Results ==========
+------------+------+------------+-----------+-----------+-----------+
| Operation  | Count| Total(ms)  | Avg(ms)   | Min(ms)   | Max(ms)   |
| mkdir      |10000 |       5000 |         0 |         0 |        50 |
| write      |10000 |     120000 |        12 |         5 |       100 |
| read       |10000 |      80000 |         8 |         3 |        80 |
| delete_dir |10000 |       6000 |         0 |         0 |        60 |
| delete_file|10000 |       4000 |         0 |         0 |        40 |
| ls         |10000 |      30000 |         3 |         1 |        30 |
+------------+------+------------+-----------+-----------+-----------+
| Operation  | Count| Errors   |
| mkdir      |10000 |        0  |
| write      |10000 |        5  |
| read       |10000 |        2  |
| delete_dir |10000 |        0  |
| delete_file|10000 |        1  |
| ls         |10000 |        0  |
+------------+------+------------+
===================================
```

### 统计指标说明
- **Operation**: 操作类型（mkdir, write, read, delete_dir, delete_file, ls）
- **Count**: 该操作执行的总次数
- **Errors**: 该操作执行失败的次数
- **Total(ms)**: 所有成功操作的总耗时（毫秒）
- **Avg(ms)**: 平均每次成功操作的耗时（毫秒）
- **Min(ms)**: 最快的一次成功操作耗时（毫秒）
- **Max(ms)**: 最慢的一次成功操作耗时（毫秒）

## 系统要求

- Java 17+
- Hadoop 3.4.2
- Maven 3.6+

## 注意事项

1. 确保测试目录（`--baseDir`）存在且有写权限
2. 根据集群资源调整 Map 和 Reduce 任务数量
3. 大文件操作需要足够的磁盘空间和内存
4. 建议先在测试环境运行，确认无误后再在生产环境使用
