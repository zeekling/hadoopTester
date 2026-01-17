package com.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

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
        if (localFs != null && testBaseDir != null) {
            localFs.delete(testBaseDir, true);
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(operation);
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
        OperationOutput result = operation.execute("read", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("read", result.getOperationType());
        assertEquals("skipped", result.getMeasurementType());
        assertEquals(0L, result.getValue());
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
        
        assertEquals(OperationOutput.OutputType.INTEGER, result.getOutputType());
        assertEquals("ls", result.getOperationType());
        assertEquals("count", result.getMeasurementType());
        assertTrue((Integer) result.getValue() >= 3);
    }

    @Test
    public void testExecuteMultipleMkdirOperations() throws Exception {
        operation.execute("mkdir", 0);
        operation.execute("mkdir", 1);
        operation.execute("mkdir", 2);
        
        Path dir0 = new Path(testBaseDir + "/mkdir/1/dir_0");
        Path dir1 = new Path(testBaseDir + "/mkdir/1/dir_1");
        Path dir2 = new Path(testBaseDir + "/mkdir/1/dir_2");
        
        assertTrue(localFs.exists(dir0));
        assertTrue(localFs.exists(dir1));
        assertTrue(localFs.exists(dir2));
    }

    @Test
    public void testExecuteInvalidOperation() {
        OperationOutput result = operation.execute("invalid_operation", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertEquals("error", result.getMeasurementType());
        assertEquals(-1L, result.getValue());
    }

    @Test
    public void testExecuteWithDurationTracking() {
        OperationOutput result = operation.execute("mkdir", 0);
        
        long duration = Long.parseLong(result.getValue().toString());
        assertTrue("Duration should be non-negative", duration >= 0);
    }

    @Test
    public void testExecuteWithDifferentIndices() throws Exception {
        operation.execute("mkdir", 0);
        operation.execute("mkdir", 5);
        operation.execute("mkdir", 10);
        
        Path dir0 = new Path(testBaseDir + "/mkdir/1/dir_0");
        Path dir5 = new Path(testBaseDir + "/mkdir/1/dir_5");
        Path dir10 = new Path(testBaseDir + "/mkdir/1/dir_10");
        
        assertTrue(localFs.exists(dir0));
        assertTrue(localFs.exists(dir5));
        assertTrue(localFs.exists(dir10));
    }

    @Test
    public void testExecuteDeleteNonExistentDirectory() throws Exception {
        Path dirPath = new Path(testBaseDir + "/mkdir/1/dir_0");
        assertFalse(localFs.exists(dirPath));
        
        OperationOutput result = operation.execute("delete_dir", 0);
        
        assertNotNull(result);
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
    }

    @Test
    public void testExecuteDeleteNonExistentFile() throws Exception {
        Path filePath = new Path(testBaseDir + "/write/1/file_0");
        assertFalse(localFs.exists(filePath));
        
        OperationOutput result = operation.execute("delete_file", 0);
        
        assertNotNull(result);
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
    }

    @Test
    public void testExecuteLsWithEmptyDirectory() throws Exception {
        Path dirPath = new Path(testBaseDir + "/write/1");
        localFs.mkdirs(dirPath);
        
        OperationOutput result = operation.execute("ls", 0);
        
        assertEquals(OperationOutput.OutputType.INTEGER, result.getOutputType());
        assertEquals(0, result.getValue());
    }

    @Test
    public void testExecuteWriteWithDifferentSizes() throws Exception {
        HdfsOperation largeOp = new HdfsOperation(localFs, testBaseDir.toString(), 2, 5);
        OperationOutput result = largeOp.execute("write", 0);
        
        assertEquals(OperationOutput.OutputType.LONG, result.getOutputType());
        assertNotNull(result.getValue());
        
        Path filePath = new Path(testBaseDir + "/write/2/file_0");
        assertTrue(localFs.getFileStatus(filePath).getLen() > 0);
    }
}
