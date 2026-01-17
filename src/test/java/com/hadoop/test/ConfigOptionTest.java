package com.hadoop.test;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigOptionTest {

    @Test
    public void testMapsDefaultValue() {
        assertEquals(Integer.valueOf(10), ConfigOption.MAPS.getDefaultValue());
    }

    @Test
    public void testReducesDefaultValue() {
        assertEquals(Integer.valueOf(1), ConfigOption.REDUCES.getDefaultValue());
    }

    @Test
    public void testBaseDirDefaultValue() {
        assertEquals("/test/hdfsrpc", ConfigOption.BASE_DIR.getDefaultValue());
    }

    @Test
    public void testOperationsDefaultValue() {
        assertEquals("mkdir,write,read,delete_dir,delete_file,ls", ConfigOption.OPERATIONS.getDefaultValue());
    }

    @Test
    public void testFileSizeDefaultValue() {
        assertEquals(Integer.valueOf(10), ConfigOption.FILE_SIZE.getDefaultValue());
    }

    @Test
    public void testFileCountDefaultValue() {
        assertEquals(Integer.valueOf(100), ConfigOption.FILE_COUNT.getDefaultValue());
    }

    @Test
    public void testOpsPerMapperDefaultValue() {
        assertEquals(Integer.valueOf(10000), ConfigOption.OPS_PER_MAPPER.getDefaultValue());
    }

    @Test
    public void testDirCountDefaultValue() {
        assertEquals(Integer.valueOf(10), ConfigOption.DIR_COUNT.getDefaultValue());
    }

    @Test
    public void testMapsCfgOption() {
        assertEquals("test.maps", ConfigOption.MAPS.getCfgOption());
    }

    @Test
    public void testReducesCfgOption() {
        assertEquals("test.reduces", ConfigOption.REDUCES.getCfgOption());
    }

    @Test
    public void testBaseDirCfgOption() {
        assertEquals("test.base.dir", ConfigOption.BASE_DIR.getCfgOption());
    }

    @Test
    public void testOperationsCfgOption() {
        assertEquals("test.operations", ConfigOption.OPERATIONS.getCfgOption());
    }

    @Test
    public void testFileSizeCfgOption() {
        assertEquals("test.file.size", ConfigOption.FILE_SIZE.getCfgOption());
    }

    @Test
    public void testFileCountCfgOption() {
        assertEquals("test.file.count", ConfigOption.FILE_COUNT.getCfgOption());
    }

    @Test
    public void testOpsPerMapperCfgOption() {
        assertEquals("test.ops.per.mapper", ConfigOption.OPS_PER_MAPPER.getCfgOption());
    }

    @Test
    public void testDirCountCfgOption() {
        assertEquals("test.dir.count", ConfigOption.DIR_COUNT.getCfgOption());
    }

    @Test
    public void testMapsHasArg() {
        assertTrue(ConfigOption.MAPS.hasArg());
    }

    @Test
    public void testReducesHasArg() {
        assertTrue(ConfigOption.REDUCES.hasArg());
    }

    @Test
    public void testBaseDirHasArg() {
        assertTrue(ConfigOption.BASE_DIR.hasArg());
    }

    @Test
    public void testOperationsHasArg() {
        assertTrue(ConfigOption.OPERATIONS.hasArg());
    }

    @Test
    public void testFileSizeHasArg() {
        assertTrue(ConfigOption.FILE_SIZE.hasArg());
    }

    @Test
    public void testFileCountHasArg() {
        assertTrue(ConfigOption.FILE_COUNT.hasArg());
    }

    @Test
    public void testOpsPerMapperHasArg() {
        assertTrue(ConfigOption.OPS_PER_MAPPER.hasArg());
    }

    @Test
    public void testDirCountHasArg() {
        assertTrue(ConfigOption.DIR_COUNT.hasArg());
    }

    @Test
    public void testMapsOpt() {
        assertEquals("maps", ConfigOption.MAPS.getOpt());
    }

    @Test
    public void testReducesOpt() {
        assertEquals("reduces", ConfigOption.REDUCES.getOpt());
    }

    @Test
    public void testBaseDirOpt() {
        assertEquals("baseDir", ConfigOption.BASE_DIR.getOpt());
    }

    @Test
    public void testOperationsOpt() {
        assertEquals("operations", ConfigOption.OPERATIONS.getOpt());
    }

    @Test
    public void testFileSizeOpt() {
        assertEquals("fileSize", ConfigOption.FILE_SIZE.getOpt());
    }

    @Test
    public void testFileCountOpt() {
        assertEquals("fileCount", ConfigOption.FILE_COUNT.getOpt());
    }

    @Test
    public void testOpsPerMapperOpt() {
        assertEquals("opsPerMapper", ConfigOption.OPS_PER_MAPPER.getOpt());
    }

    @Test
    public void testDirCountOpt() {
        assertEquals("dirCount", ConfigOption.DIR_COUNT.getOpt());
    }
}
