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
    public void testExecuteMkdir() {
        OperationOutput result = operation.execute("mkdir", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("mkdir", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertNotNull(result.getValue());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
    }

    @Test
    public void testExecuteMkdirCreatesDirectory() throws IOException {
        Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_0");
        assertFalse(localFs.exists(dirPath));
        
        operation.execute("mkdir", 0);
        
        assertTrue(localFs.exists(dirPath));
        assertTrue(localFs.isDirectory(dirPath));
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
    public void testExecuteDeleteDir() throws IOException {
        Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_0");
        
        operation.execute("mkdir", 0);
        assertTrue(localFs.exists(dirPath));
        
        OperationOutput result = operation.execute("delete_dir", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("delete_dir", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());
        assertTrue(Long.parseLong(result.getValue().toString()) >= 0);
        
        assertFalse(localFs.exists(dirPath));
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
    public void testExecuteLs() throws IOException {
        operation.execute("write", 0);
        operation.execute("write", 1);
        operation.execute("write", 2);
        
        OperationOutput result = operation.execute("ls", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("ls", result.getOperationType());
        assertTrue((Long) result.getValue() >= 0);
    }

    @Test
    public void testExecuteLsReturnsCount() throws IOException {
        operation.execute("write", 0);
        operation.execute("write", 1);
        operation.execute("write", 2);

        Path dirPath = new Path(testBaseDir + "/write/1");
        int expectedCount = dirPath.getFileSystem(new Configuration()).listStatus(dirPath).length;

        OperationOutput result = operation.execute("ls", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("ls", result.getOperationType());
    }

    @Test
    public void testExecuteLsWithEmptyDirectory() throws Exception {
        Path dirPath = new Path(testBaseDir + "/write/1");
        localFs.mkdirs(dirPath);

        OperationOutput result = operation.execute("ls", 0);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("ls", result.getOperationType());
        assertTrue((Long) result.getValue() >= 0);
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
    public void testExecuteAsyncMkdir() throws Exception {
        CompletableFuture<OperationOutput> future = operation.executeAsync("mkdir", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("mkdir", result.getOperationType());
        assertEquals("duration", result.getMeasurementType());

        Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_0");
        assertTrue(localFs.exists(dirPath));
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
    public void testExecuteAsyncDeleteDir() throws Exception {
        operation.execute("mkdir", 0);

        Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_0");
        assertTrue(localFs.exists(dirPath));

        CompletableFuture<OperationOutput> future = operation.executeAsync("delete_dir", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("delete_dir", result.getOperationType());
        assertFalse(localFs.exists(dirPath));
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
    public void testExecuteAsyncLs() throws Exception {
        operation.execute("write", 0);
        operation.execute("write", 1);
        operation.execute("write", 2);

        CompletableFuture<OperationOutput> future = operation.executeAsync("ls", 0);
        OperationOutput result = future.get(10, TimeUnit.SECONDS);

        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("ls", result.getOperationType());
        assertTrue((Long) result.getValue() >= 0);
    }

    @Test
    public void testExecuteAsyncMultipleOperations() throws Exception {
        CompletableFuture<OperationOutput> mkdirFuture = operation.executeAsync("mkdir", 0);
        CompletableFuture<OperationOutput> writeFuture = operation.executeAsync("write", 0);
        CompletableFuture<OperationOutput> readFuture = operation.executeAsync("read", 0);

        CompletableFuture.allOf(mkdirFuture, writeFuture, readFuture).get(30, TimeUnit.SECONDS);

        assertTrue(mkdirFuture.isDone());
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
        operation.executeAsync("mkdir", index).get(10, TimeUnit.SECONDS);
        operation.execute("mkdir", index + 1);

        Path dir0 = new Path(testBaseDir + "/mkdir/1/dir_" + index);
        Path dir1 = new Path(testBaseDir + "/mkdir/1/dir_" + (index + 1));

        assertTrue(localFs.exists(dir0));
        assertTrue(localFs.exists(dir1));
    }

    @Test
    public void testExecuteAsyncParallel() throws Exception {
        int count = 10;
        List<CompletableFuture<OperationOutput>> futures = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            futures.add(operation.executeAsync("mkdir", i));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);

        for (int i = 0; i < count; i++) {
            Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_" + i);
            assertTrue(localFs.exists(dirPath));
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
}
