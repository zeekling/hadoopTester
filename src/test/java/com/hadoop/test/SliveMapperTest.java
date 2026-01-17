package com.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SliveMapperTest {

    private SliveMapper mapper;
    private JobConf conf;
    private OutputCollector<Text, Text> output;
    private Reporter reporter;
    private FileSystem localFs;
    private Path testBaseDir;

    @Before
    public void setUp() throws Exception {
        mapper = new SliveMapper();
        conf = new JobConf();
        testBaseDir = new Path("target/test-data/slivemapper-" + System.currentTimeMillis());
        
        localFs = FileSystem.getLocal(new Configuration());
        localFs.mkdirs(testBaseDir);
        
        conf.set(ConfigOption.BASE_DIR.getCfgOption(), testBaseDir.toString());
        conf.set(ConfigOption.OPERATIONS.getCfgOption(), "mkdir,write");
        conf.setInt(ConfigOption.FILE_SIZE.getCfgOption(), 1);
        conf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 10);
        conf.set("mapred.task.id", "attempt_001_0001_m_000000_0");
        
        mapper.configure(conf);
        
        output = mock(OutputCollector.class);
        reporter = mock(Reporter.class);
        
        List<Text> collectedKeys = new ArrayList<>();
        List<Text> collectedValues = new ArrayList<>();
        
        doAnswer(invocation -> {
            collectedKeys.add(invocation.getArgument(0));
            collectedValues.add(invocation.getArgument(1));
            return null;
        }).when(output).collect(any(Text.class), any(Text.class));
    }

    @After
    public void tearDown() throws Exception {
        if (localFs != null && testBaseDir != null) {
            localFs.delete(testBaseDir, true);
        }
    }

    @Test
    public void testConfigure() {
        SliveMapper newMapper = new SliveMapper();
        newMapper.configure(conf);
        assertNotNull(newMapper);
    }

    @Test
    public void testMapWithDummyInput() throws IOException {
        Object key = "test_key";
        Object value = "test_value";
        
        mapper.map(key, value, output, reporter);
        
        verify(output, times(10)).collect(any(Text.class), any(Text.class));
        verify(reporter, atLeast(2)).setStatus(anyString());
    }

    @Test
    public void testMapOutputFormat() throws IOException {
        Object key = "test_key";
        Object value = "test_value";
        
        mapper.map(key, value, output, reporter);
        
        verify(output, atLeastOnce()).collect(argThat(text -> text.toString().equals("mkdir") || text.toString().equals("write")),
                               any(Text.class));
    }

    @Test
    public void testMapWithDuration() throws IOException {
        Object key = "test_key";
        Object value = "test_value";
        
        mapper.map(key, value, output, reporter);
        
        verify(output, times(10)).collect(any(Text.class), argThat(text -> {
            try {
                Long.parseLong(text.toString());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }));
    }

    @Test
    public void testMapStatusUpdates() throws IOException {
        Object key = "test_key";
        Object value = "test_value";

        conf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 200);
        mapper.configure(conf);

        mapper.map(key, value, output, reporter);

        verify(reporter).setStatus(contains("Running slive mapper"));
        verify(reporter).setStatus("Completed 100 operations");
        verify(reporter).setStatus(contains("Completed 200 operations successfully, 0 failed"));
    }

    @Test
    public void testMapWithMultipleOperations() throws IOException {
        conf.set(ConfigOption.OPERATIONS.getCfgOption(), "mkdir,write,read,delete_dir,delete_file,ls");
        conf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 6);
        mapper.configure(conf);
        
        Object key = "test_key";
        Object value = "test_value";
        
        mapper.map(key, value, output, reporter);
        
        verify(output, times(6)).collect(any(Text.class), any(Text.class));
    }

    @Test
    public void testMapWithSingleOperation() throws IOException {
        conf.set(ConfigOption.OPERATIONS.getCfgOption(), "mkdir");
        conf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 5);
        mapper.configure(conf);
        
        Object key = "test_key";
        Object value = "test_value";
        
        mapper.map(key, value, output, reporter);
        
        verify(output, times(5)).collect(argThat(text -> text.toString().equals("mkdir")), any(Text.class));
    }

    @Test
    public void testConfigureWithNullBaseDir() throws Exception {
        JobConf nullConf = new JobConf();
        SliveMapper newMapper = new SliveMapper();

        try {
            newMapper.configure(nullConf);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Unable to setup slive configuration"));
        }
    }

    @Test
    public void testConfigureWithThreadPoolSize() throws Exception {
        JobConf testConf = new JobConf();
        testConf.set(ConfigOption.BASE_DIR.getCfgOption(), testBaseDir.toString());
        testConf.set(ConfigOption.OPERATIONS.getCfgOption(), "mkdir");
        testConf.setInt(ConfigOption.THREAD_POOL_SIZE.getCfgOption(), 5);
        testConf.setInt(ConfigOption.FILE_SIZE.getCfgOption(), 1);
        testConf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 10);
        testConf.set("mapred.task.id", "attempt_001_0001_m_000000_0");

        SliveMapper testMapper = new SliveMapper();
        testMapper.configure(testConf);

        assertNotNull(testMapper);
    }
}
