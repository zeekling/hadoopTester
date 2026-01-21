package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class OperationGeneratorMapperTest {

    private OperationGeneratorMapper mapper;

    @Mock
    private OutputCollector<Text, Text> output;

    @Mock
    private Reporter reporter;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mapper = new OperationGeneratorMapper();
    }

    @Test
    public void testConfigure() throws IOException {
        JobConf conf = new JobConf();
        conf.set(ConfigOption.OPERATIONS.getCfgOption(), "write,read,delete");
        conf.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), 10);
        conf.set("mapred.task.id", "attempt_001_0001_m_000000_0");

        mapper.configure(conf);
        mapper.map(null, null, output, reporter);

        verify(reporter, atLeastOnce()).setStatus(any());
    }

}
