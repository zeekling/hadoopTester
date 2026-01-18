# AGENTS.md

This file contains guidelines and commands for agentic coding agents working in this Hadoop testing repository.

## Build System & Commands

This is a Maven-based Java project. Use these commands for development:

### Core Commands
- **Build project**: `mvn clean compile`
- **Run tests**: `mvn test`
- **Package JAR**: `mvn clean package`
- **Copy dependencies**: `mvn dependency:copy-dependencies` (copies to target/lib/)
- **Run main class**: `mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest"`
- **Run with arguments**: `mvn exec:java -Dexec.mainClass="com.hadoop.test.HDFSRpcTest" -Dexec.args="--maps 5 --reduces 2 --baseDir /test/path"`

### Test Commands
- **Run all tests**: `mvn test`
- **Run specific test class**: `mvn test -Dtest=ClassName`
- **Run specific test method**: `mvn test -Dtest=ClassName#methodName`
- **Skip tests during build**: `mvn clean package -DskipTests`

### Lint/Typecheck
No explicit lint or typecheck plugins are configured. Before committing changes, ensure all tests pass with `mvn test`.
- Use `@SuppressWarnings("resource")` on @Before/@After methods that delete test directories

## Project Structure

```
src/main/java/com/hadoop/test/
├── ArgumentParser.java      # CLI argument parsing
├── HDFSRpcTest.java        # Main entry point, implements Tool
├── ConfigOption.java       # Configuration options with defaults
├── Constants.java          # Application constants
├── HdfsOperation.java      # HDFS operation implementations (12 ops)
├── SliveMapper.java        # Hadoop mapper implementation
├── SliveReducer.java       # Hadoop reducer implementation
├── SlivePartitioner.java   # Custom partitioner
├── DummyInputFormat.java   # Input format for testing
└── OperationOutput.java    # Output data structure
```

## Code Style Guidelines

### Package & Imports
- Package: `com.hadoop.test`
- Import order: Standard Java libraries, third-party libraries, project imports
- Use wildcard imports sparingly, prefer explicit imports
- Lombok imports grouped with other annotations

### Naming Conventions
- **Classes**: PascalCase (e.g., `ArgumentParser`, `HDFSRpcTest`)
- **Methods**: camelCase (e.g., `parse()`, `getOutputPath()`)
- **Variables**: camelCase (e.g., `optList`, `parsedData`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `PROG_NAME`, `TYPE_SEP`)
- **Private fields**: camelCase, no Hungarian notation
- **Static final fields**: UPPER_SNAKE_CASE

### Class Structure
1. Package declaration, 2. Imports (grouped/sorted), 3. Class-level Javadoc, 4. Class declaration
5. Static fields (constants first), 6. Instance fields, 7. Constructors, 8. Static methods
9. Instance methods (public, then protected, then private), 10. Inner/nested classes

### Lombok Usage
- Use `@Getter` for fields that need getters
- Use `@Setter` only when necessary
- Prefer explicit methods for complex logic over Lombok shortcuts
- Place Lombok annotations at field level or class level as appropriate

### Error Handling
- Use specific exception types, avoid generic `Exception`
- Log errors using SLF4J: `LOG.error("Error message", exception)`
- Wrap checked exceptions in RuntimeException when appropriate
- Use `IllegalArgumentException` for invalid parameters
- Include meaningful error messages with context

### Logging
- Use SLF4J with `LoggerFactory.getLogger(ClassName.class)`
- Field name: `private static final Logger LOG`
- Log levels: ERROR for errors, WARN for warnings, INFO for important events, DEBUG for debugging
- Include context in log messages, avoid string concatenation in hot paths

### Hadoop-Specific Patterns
- Implement `Tool` interface for main applications, use `ToolRunner.run()` for entry point
- Configuration objects should be passed, not stored statically
- Use Hadoop's `Text` class instead of `String` for keys/values
- Follow Hadoop's serialization patterns for custom types
- Memory configs (mapMemoryMb, reduceMemoryMb) use String type set directly to Hadoop config

### Comments & Documentation
- Use Javadoc for public classes and methods
- Include `@param`, `@return`, and `@throws` where applicable
- Use inline comments sparingly, prefer self-documenting code
- Document complex business logic or non-obvious implementations

### Code Formatting
- Indentation: 4 spaces (no tabs)
- Line length: Prefer under 120 characters
- Braces: K&R style (opening brace on same line)
- Spacing: Single blank line between methods, logical groups
- No trailing whitespace

### Dependencies Management
- Hadoop 3.4.2, Java 17 (maven.compiler.release)
- Lombok 1.18.42 for annotations, JUnit 4.13.2, Mockito 4.11.0 for testing

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
- Operations: mkdir, write, read, delete_dir, delete_file, ls, rename, get_file_status, exists, set_permission, append, create_symlink

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
```

## Development Notes

- The project uses Hadoop MapReduce (old API) with `mapred` package
- Lombok is used for reducing boilerplate code with `@Getter` annotations
- Use `DummyInputFormat` for testing without real input data
- Configuration options should have sensible defaults
- Log at appropriate levels, avoid excessive DEBUG logging in production
- Test results use table format with `String.format` for aligned columns
- Mockito is used for testing with `verify()` and `any()` matchers
- HDFS operations support 12 different operation types for comprehensive RPC testing
