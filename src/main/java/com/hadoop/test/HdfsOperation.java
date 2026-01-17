package com.hadoop.test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
                default:
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
            }
        } catch (Exception e) {
            LOG.error("Error executing operation {} at index {}", operationType, index, e);
            return new OperationOutput(OperationOutput.OutputType.LONG, operationType, "error", -1L);
        }
    }

    private OperationOutput executeMkdir(int index, long startTime) {
        try {
            Path dirPath = new Path(baseDir + "/mkdir/" + taskId + "/dir_" + index);
            boolean success = fs.mkdirs(dirPath);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "mkdir", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute mkdir at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "mkdir", "error", duration, 1);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Unexpected error in mkdir at index {}: {}", index, e.getMessage(), e);
            return new OperationOutput(OperationOutput.OutputType.LONG, "mkdir", "error", duration, 1);
        }
    }

    private OperationOutput executeWrite(int index, long startTime) {
        FSDataOutputStream out = null;
        try {
            Path filePath = new Path(baseDir + "/write/" + taskId + "/file_" + index);
            byte[] data = generateData(fileSize * 1024 * 1024);

            out = fs.create(filePath, true);

            out.write(data);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "write", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute write at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "write", "error", duration, 1);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("Unexpected error in write at index {}: {}", index, e.getMessage(), e);
            return new OperationOutput(OperationOutput.OutputType.LONG, "write", "error", duration, 1);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close output stream: {}", e.getMessage());
                }
            }
        }
    }

    private OperationOutput executeRead(int index, long startTime) {
        FSDataInputStream in = null;
        try {
            Path filePath = new Path(baseDir + "/write/" + taskId + "/file_" + index);
            in = fs.open(filePath);


            byte[] buffer = new byte[8192];
            int totalRead = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                totalRead += bytesRead;
            }
            LOG.debug("Read {} bytes from file {}", totalRead, filePath);

            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "read", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute read at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "read", "error", duration, 1);
        } catch (Exception e) {
            LOG.error("Unexpected error in read at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "read", "error", duration, 1);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close input stream: {}", e.getMessage());
                }
            }
        }
    }

    private OperationOutput executeDeleteDir(int index, long startTime) {
        try {
            Path dirPath = new Path(baseDir + "/mkdir/" + taskId + "/dir_" + index);

            boolean success = fs.delete(dirPath, true);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_dir", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute delete_dir at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_dir", "error", duration, 1);
        } catch (Exception e) {
            LOG.error("Unexpected error in delete_dir at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_dir", "error", duration, 1);
        }
    }

    private OperationOutput executeDeleteFile(int index, long startTime) {
        try {
            Path filePath = new Path(baseDir + "/write/" + taskId + "/file_" + index);

            boolean success = fs.delete(filePath, false);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_file", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute delete_file at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_file", "error", duration, 1);
        } catch (Exception e) {
            LOG.error("Unexpected error in delete_file at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "delete_file", "error", duration, 1);
        }
    }

    private OperationOutput executeList(int index, long startTime) {
        try {
            Path dirPath = new Path(baseDir + "/write/" + taskId);

            FileStatus[] statuses = fs.listStatus(dirPath);

            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "ls", "duration", duration, 1);
        } catch (IOException e) {
            LOG.error("Failed to execute ls at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "ls", "error", duration, 1);
        } catch (Exception e) {
            LOG.error("Unexpected error in ls at index {}: {}", index, e.getMessage(), e);
            long duration = System.currentTimeMillis() - startTime;
            return new OperationOutput(OperationOutput.OutputType.LONG, "ls", "error", duration, 1);
        }
    }

    private byte[] generateData(int size) {
        byte[] data = new byte[size];
        RANDOM.nextBytes(data);
        return data;
    }
}
