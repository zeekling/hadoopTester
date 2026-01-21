package com.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class HdfsOperationTest {

    private HdfsOperation operation;
    private FileSystem localFs;
    private Path testBaseDir;

    @Before
    public void setUp() throws Exception {
        testBaseDir = new Path("target/test-data/hdfsop-" + System.currentTimeMillis());
        localFs = FileSystem.getLocal(new Configuration());
        localFs.mkdirs(testBaseDir);

        operation = new HdfsOperation(localFs, testBaseDir.toString(), 1, 1);
    }

    @After
    public void tearDown() throws Exception {
        if (operation != null) {
            operation.shutdown();
        }
        if (localFs != null && testBaseDir != null) {
            localFs.delete(testBaseDir, true);
        }
    }

    @Test
    public void testConstructorWithThreadPoolSize() {
        HdfsOperation op = new HdfsOperation(localFs, testBaseDir.toString(), 1, 20);
        assertNotNull(op);
        assertNotNull(op.executorService);
        op.shutdown();
    }

    @Test
    public void testConstructorDefaultThreadPoolSize() {
        HdfsOperation op = new HdfsOperation(localFs, testBaseDir.toString(), 1, 1);
        assertNotNull(op);
        assertNotNull(op.executorService);
        op.shutdown();
    }

    @Test
    public void testExecuteWrite() {
        OperationOutput result = operation.execute("write", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("write", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteWriteCreatesFile() throws IOException {
        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        assertFalse(localFs.exists(filePath));
        
        operation.execute("write", 0);
        
        assertTrue(localFs.exists(filePath));
        assertTrue(localFs.getFileStatus(filePath).isFile());
    }

    @Test
    public void testExecuteWriteWithNonExistentFile() {
        operation.execute("write", 0);
        
        OperationOutput result = operation.execute("write", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("write", result.getOperationType());
    }

    @Test
    public void testExecuteReadAfterWrite() throws IOException {
        Path filePath = new Path(testBaseDir + "/write/1/file_0");

        operation.execute("write", 0);
        assertTrue(localFs.exists(filePath));

        OperationOutput result = operation.execute("read", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("read", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteDeleteFile() throws IOException {
        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        
        operation.execute("write", 0);
        assertTrue(localFs.exists(filePath));
        
        OperationOutput result = operation.execute("delete_file", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("delete_file", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);

        assertFalse(localFs.exists(filePath));
    }

    @Test
    public void testConstructor() {
        assertNotNull(operation);
    }

    @Test
    public void testExecuteInvalidOperation() {
        OperationOutput result = operation.execute("invalid_op", 0);

        assertNotNull(result);
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("error", result.getMeasurementType());
        assertEquals(-1L, result.getValue());
    }

    @Test
    public void testExecuteAsyncWrite() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("write", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("write", result.getOperationType());

        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        assertTrue(localFs.exists(filePath));
    }

    @Test
    public void testExecuteAsyncRead() throws Exception {
        operation.execute("write", 0);

        CompletableFuture<OperationOutput> future = operation.executeAsync("read", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("read", result.getOperationType());
    }

    @Test
    public void testExecuteAsyncDeleteFile() throws Exception {
        operation.execute("write", 0);

        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        assertTrue(localFs.exists(filePath));

        CompletableFuture<OperationOutput> future = operation.executeAsync("delete_file", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("delete_file", result.getOperationType());
        assertFalse(localFs.exists(filePath));
    }

    @Test
    public void testExecuteAsyncMultipleOperations() throws Exception {
        CompletableFuture<OperationOutput> renameFuture = operation.executeAsync("rename", 0);
        CompletableFuture<OperationOutput> writeFuture = operation.executeAsync("write", 1);
        CompletableFuture<OperationOutput> readFuture = operation.executeAsync("read", 0);

        CompletableFuture.allOf(renameFuture, writeFuture, readFuture).get(30, TimeUnit.SECONDS);

        assertTrue(renameFuture.isDone());
        assertTrue(writeFuture.isDone());
        assertTrue(readFuture.isDone());
    }

    @Test
    public void testExecuteAsyncInvalidOperation() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("invalid_operation", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("error", result.getMeasurementType());
        assertEquals(-1L, result.getValue());
    }

    @Test(expected = TimeoutException.class)
    public void testExecuteAsyncTimeout() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("write", 0);
        future.get(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testExecuteAsyncConsistencyWithSync() throws Exception {
        int index = 100;
        operation.executeAsync("write", index).get(10, TimeUnit.SECONDS);
        operation.execute("write", index + 1);

        Path file0 = new Path(testBaseDir + "/write/1/file_" + index);
        Path file1 = new Path(testBaseDir + "/write/1/file_" + (index + 1));

        assertTrue(localFs.exists(file0));
        assertTrue(localFs.exists(file1));
    }

    @Test
    public void testExecuteAsyncParallel() throws Exception {
        int count = 10;
        List<CompletableFuture<OperationOutput>> futures = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            futures.add(operation.executeAsync("write", i));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);

        for (int i = 0; i < count; i++) {
            Path filePath = new Path(testBaseDir + "/write/1/file_" + i);
            assertTrue(localFs.exists(filePath));
        }
    }

    @Test
    public void testShutdown() throws Exception {
        HdfsOperation op = new HdfsOperation(localFs, testBaseDir.toString(), 2, 1);
        op.shutdown();
        assertTrue("Executor should be shut down", op.executorService.isShutdown());
    }

    @Test
    public void testExecuteAsyncWithError() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("invalid_op", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("error", result.getMeasurementType());
    }

    @Test
    public void testExecuteRename() throws IOException {
        operation.execute("write", 0);

        Path oldPath = new Path(testBaseDir + "/write/1/file_0");
        assertTrue(localFs.exists(oldPath));

        OperationOutput result = operation.execute("rename", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("rename", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);

        Path newPath = new Path(testBaseDir + "/write/1/file_renamed_0");
        assertFalse(localFs.exists(oldPath));
        assertTrue(localFs.exists(newPath));
    }

    @Test
    public void testExecuteGetFileStatus() {
        operation.execute("write", 0);

        OperationOutput result = operation.execute("get_file_status", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("get_file_status", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteExists() {
        operation.execute("write", 0);

        OperationOutput result = operation.execute("exists", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("exists", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteExistsNonExistent() {
        OperationOutput result = operation.execute("exists", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("exists", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteSetPermission() {
        operation.execute("write", 0);

        OperationOutput result = operation.execute("set_permission", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("set_permission", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteAppend() throws IOException {
        operation.execute("write", 0);

        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        assertTrue(localFs.exists(filePath));

        OperationOutput result = operation.execute("append", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("append", result.getOperationType());
        assertNotNull(result.getValue());
    }

    @Test
    public void testExecuteCreateSymlink() throws IOException {
        operation.execute("write", 0);

        Path targetPath = new Path(testBaseDir + "/write/1/file_0");
        assertTrue(localFs.exists(targetPath));

        Path linkDir = new Path(testBaseDir + "/link_1");
        localFs.mkdirs(linkDir);

        OperationOutput result = operation.execute("create_symlink", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("create_symlink", result.getOperationType());
        assertNotNull(result.getValue());
    }

    @Test
    public void testExecuteAsyncRename() throws Exception {
        operation.execute("write", 0);

        Path oldPath = new Path(testBaseDir + "/write/1/file_0");
        CompletableFuture<OperationOutput> future = operation.executeAsync("rename", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("rename", result.getOperationType());

        Path newPath = new Path(testBaseDir + "/write/1/file_renamed_0");
        assertFalse(localFs.exists(oldPath));
        assertTrue(localFs.exists(newPath));
    }

    @Test
    public void testExecuteAsyncAppend() throws Exception {
        operation.execute("write", 0);

        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        CompletableFuture<OperationOutput> future = operation.executeAsync("append", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("append", result.getOperationType());

        assertTrue(localFs.exists(filePath));
    }

    @Test
    public void testExecuteAsyncSetPermission() throws Exception {
        operation.execute("write", 0);

        CompletableFuture<OperationOutput> future = operation.executeAsync("set_permission", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("set_permission", result.getOperationType());
    }

    @Test
    public void testExecuteAppendTruncate() throws Exception {
        OperationOutput result = operation.execute("append_truncate", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("append_truncate", result.getOperationType());
        assertNotNull(result.getValue());
        assertTrue((Long) result.getValue() >= 0);

        Path filePath = new Path(testBaseDir + "/append_truncate/1/file_0");
        assertTrue(localFs.exists(filePath));
    }

    @Test
    public void testExecuteAsyncAppendTruncate() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("append_truncate", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("append_truncate", result.getOperationType());
    }
}
