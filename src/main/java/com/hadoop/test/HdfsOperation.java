package com.hadoop.test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsOperation {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsOperation.class);
    private static final Random RANDOM = new Random();

    private static final int BUFFER_SIZE = 8192;
    private static final int APPEND_DATA_SIZE = 1024;
    private static final int APPEND_TRUNCATE_DATA_SIZE = 512;
    private static final int APPEND_TRUNCATE_ITERATIONS = 10;
    private static final int APPEND_TRUNCATE_TRUNCATE_SIZE = 500;
    private static final short FILE_PERMISSION = 0644;

    private static final String DURATION = "duration";
    private static final String ERROR = "error";

    private final FileSystem fs;
    private final String baseDir;
    private final int taskId;
    private final int fileSize;
    final ExecutorService executorService;

    public HdfsOperation(FileSystem fs, String baseDir, int taskId, int fileSize) {
        this(fs, baseDir, taskId, fileSize, 10);
    }

    public HdfsOperation(FileSystem fs, String baseDir, int taskId, int fileSize, int threadPoolSize) {
        this.fs = fs;
        this.baseDir = baseDir;
        this.taskId = taskId;
        this.fileSize = fileSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }

    public CompletableFuture<OperationOutput> executeAsync(String operationType, int index) {
        return CompletableFuture.supplyAsync(() -> execute(operationType, index), executorService);
    }

    public OperationOutput execute(String operationType, int index) {
        try {
            long startTime = System.currentTimeMillis();
            switch (operationType) {
                case "mkdir":
                    return executeMkdir(index, startTime);
                case "write":
                    return executeWrite(index, startTime);
                case "read":
                    return executeRead(index, startTime);
                case "delete_dir":
                    return executeDeleteDir(index, startTime);
                case "delete_file":
                    return executeDeleteFile(index, startTime);
                case "ls":
                    return executeList(index, startTime);
                case "rename":
                    return executeRename(index, startTime);
                case "get_file_status":
                    return executeGetFileStatus(index, startTime);
                case "exists":
                    return executeExists(index, startTime);
                case "set_permission":
                    return executeSetPermission(index, startTime);
                case "append":
                    return executeAppend(index, startTime);
                case "create_symlink":
                    return executeCreateSymlink(index, startTime);
                case "append_truncate":
                    return executeAppendTruncate(index, startTime);
                default:
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
            }
        } catch (IllegalArgumentException e) {
            LOG.error("Unknown operation type: {}", operationType, e);
            return new OperationOutput(OperationOutput.OutputType.LONG, operationType, ERROR, -1L);
        } catch (Exception e) {
            LOG.error("Error executing operation {} at index {}", operationType, index, e);
            return createErrorOutput(operationType, 0);
        }
    }

    private OperationOutput executeMkdir(int index, long startTime) {
        return executeOperation("mkdir", index, startTime, () -> {
            Path dirPath = buildPath("mkdir", "dir_" + index);
            fs.mkdirs(dirPath);
        });
    }

    private OperationOutput executeWrite(int index, long startTime) {
        FSDataOutputStream out = null;
        try {
            Path filePath = buildPath("write", "file_" + index);
            byte[] data = generateData(fileSize * 1024 * 1024);
            out = fs.create(filePath, true);
            out.write(data);
            return createSuccessOutput("write", startTime);
        } catch (IOException e) {
            LOG.error("Failed to execute write at index {}: {}", index, e.getMessage(), e);
            return createErrorOutput("write", startTime);
        } finally {
            closeQuietly(out);
        }
    }

    private OperationOutput executeRead(int index, long startTime) {
        FSDataInputStream in = null;
        try {
            Path filePath = buildPath("write", "file_" + index);
            in = fs.open(filePath);
            byte[] buffer = new byte[BUFFER_SIZE];
            int totalRead = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                totalRead += bytesRead;
            }
            LOG.debug("Read {} bytes from file {}", totalRead, filePath);
            return createSuccessOutput("read", startTime);
        } catch (IOException e) {
            LOG.error("Failed to execute read at index {}: {}", index, e.getMessage(), e);
            return createErrorOutput("read", startTime);
        } finally {
            closeQuietly(in);
        }
    }

    private OperationOutput executeDeleteDir(int index, long startTime) {
        return executeOperation("delete_dir", index, startTime, () -> {
            Path dirPath = buildPath("mkdir", "dir_" + index);
            fs.delete(dirPath, true);
        });
    }

    private OperationOutput executeDeleteFile(int index, long startTime) {
        return executeOperation("delete_file", index, startTime, () -> {
            Path filePath = buildPath("write", "file_" + index);
            fs.delete(filePath, false);
        });
    }

    private OperationOutput executeList(int index, long startTime) {
        return executeOperation("ls", index, startTime, () -> {
            Path dirPath = buildPath("write", "");
            fs.listStatus(dirPath);
        });
    }

    private OperationOutput executeRename(int index, long startTime) {
        return executeOperation("rename", index, startTime, () -> {
            Path oldPath = buildPath("write", "file_" + index);
            Path newPath = buildPath("write", "file_renamed_" + index);
            fs.rename(oldPath, newPath);
        });
    }

    private OperationOutput executeGetFileStatus(int index, long startTime) {
        return executeOperation("get_file_status", index, startTime, () -> {
            Path filePath = buildPath("write", "file_" + index);
            fs.getFileStatus(filePath);
        });
    }

    private OperationOutput executeExists(int index, long startTime) {
        return executeOperation("exists", index, startTime, () -> {
            Path filePath = buildPath("write", "file_" + index);
            fs.exists(filePath);
        });
    }

    private OperationOutput executeSetPermission(int index, long startTime) {
        return executeOperation("set_permission", index, startTime, () -> {
            Path filePath = buildPath("write", "file_" + index);
            fs.setPermission(filePath, new FsPermission(FILE_PERMISSION));
        });
    }

    private OperationOutput executeAppend(int index, long startTime) {
        FSDataOutputStream out = null;
        try {
            Path filePath = buildPath("write", "file_" + index);
            byte[] data = generateData(APPEND_DATA_SIZE);
            out = fs.append(filePath);
            out.write(data);
            return createSuccessOutput("append", startTime);
        } catch (IOException e) {
            LOG.error("Failed to execute append at index {}: {}", index, e.getMessage(), e);
            return createErrorOutput("append", startTime);
        } finally {
            closeQuietly(out);
        }
    }

    private OperationOutput executeCreateSymlink(int index, long startTime) {
        return executeOperation("create_symlink", index, startTime, () -> {
            Path targetPath = buildPath("write", "file_" + index);
            Path linkPath = new Path(baseDir + "/link_" + taskId + "/link_" + index);
            fs.createSymlink(targetPath, linkPath, false);
        });
    }

    private OperationOutput executeAppendTruncate(int index, long startTime) {
        FSDataOutputStream out = null;
        try {
            Path filePath = buildPath("append_truncate", "file_" + index);
            Path parentDir = new Path(baseDir + "/append_truncate/" + taskId);

            if (!fs.exists(parentDir)) {
                fs.mkdirs(parentDir);
            }

            if (!fs.exists(filePath)) {
                byte[] initialData = generateData(APPEND_DATA_SIZE);
                out = fs.create(filePath, true);
                out.write(initialData);
            } else {
                out = fs.append(filePath);
            }

            for (int i = 0; i < APPEND_TRUNCATE_ITERATIONS; i++) {
                byte[] data = generateData(APPEND_TRUNCATE_DATA_SIZE);
                out.write(data);
                out.flush();
                out.close();
                fs.truncate(filePath, APPEND_TRUNCATE_TRUNCATE_SIZE);
            }

            return createSuccessOutput("append_truncate", startTime);
        } catch (IOException e) {
            LOG.error("Failed to execute append_truncate at index {}: {}", index, e.getMessage(), e);
            return createErrorOutput("append_truncate", startTime);
        } finally {
            closeQuietly(out);
        }
    }

    private OperationOutput executeOperation(String operationType, int index, long startTime, OperationRunnable operation) {
        try {
            operation.run();
            return createSuccessOutput(operationType, startTime);
        } catch (Exception e) {
            LOG.error("Failed to execute {} at index {}: {}", operationType, index, e.getMessage(), e);
            return createErrorOutput(operationType, startTime);
        }
    }

    private OperationOutput createSuccessOutput(String operationType, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        return new OperationOutput(OperationOutput.OutputType.LONG, operationType, DURATION, duration, 1);
    }

    private OperationOutput createErrorOutput(String operationType, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        return new OperationOutput(OperationOutput.OutputType.LONG, operationType, ERROR, duration, 1);
    }

    private Path buildPath(String type, String suffix) {
        return new Path(baseDir + "/" + type + "/" + taskId + "/" + suffix);
    }

    private byte[] generateData(int size) {
        byte[] data = new byte[size];
        RANDOM.nextBytes(data);
        return data;
    }

    private void closeQuietly(FSDataOutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                LOG.warn("Failed to close output stream: {}", e.getMessage());
            }
        }
    }

    private void closeQuietly(FSDataInputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                LOG.warn("Failed to close input stream: {}", e.getMessage());
            }
        }
    }

    @FunctionalInterface
    private interface OperationRunnable {
        void run() throws Exception;
    }
}
