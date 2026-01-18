package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SliveReducerTest {

    private SliveReducer reducer;
    private OutputCollector<Text, Text> output;
    private Reporter reporter;
    private List<Text> collectedKeys;
    private List<Text> collectedValues;

    @Before
    public void setUp() throws Exception {
        reducer = new SliveReducer();
        reducer.configure(new JobConf());

        output = mock(OutputCollector.class);
        reporter = mock(Reporter.class);

        collectedKeys = new ArrayList<>();
        collectedValues = new ArrayList<>();

        doAnswer(invocation -> {
            collectedKeys.add(invocation.getArgument(0));
            collectedValues.add(invocation.getArgument(1));
            return null;
        }).when(output).collect(any(Text.class), any(Text.class));
    }

    @Test
    public void testConfigure() {
        SliveReducer newReducer = new SliveReducer();
        newReducer.configure(new JobConf());
        assertNotNull(newReducer);
    }

    @Test
    public void testReduceWithSingleValue() throws IOException {
        Text key = new Text("mkdir");
        List<Text> values = Arrays.asList(new Text("100"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        assertEquals(1, collectedKeys.size());
        assertEquals("mkdir", collectedKeys.get(0).toString());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("| mkdir      |    1 |"));
        assertTrue(result.contains("100 |"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceWithMultipleValues() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(
            new Text("10"),
            new Text("20"),
            new Text("30")
        );
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        assertEquals(1, collectedKeys.size());
        assertEquals("write", collectedKeys.get(0).toString());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("| write      |    3 |"));
        assertTrue(result.contains("60"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceWithLargeValues() throws IOException {
        Text key = new Text("read");
        List<Text> values = Arrays.asList(
            new Text("100"),
            new Text("200"),
            new Text("150"),
            new Text("50"),
            new Text("300")
        );
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("800"));
        assertTrue(result.contains("160"));
        assertTrue(result.contains("50"));
        assertTrue(result.contains("300"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceWithDifferentOperations() throws IOException {
        String[] operations = {"mkdir", "write", "read", "delete_dir", "delete_file", "ls"};
        
        for (String op : operations) {
            collectedKeys.clear();
            collectedValues.clear();
            
            Text key = new Text(op);
            List<Text> values = Arrays.asList(new Text("100"), new Text("200"));
            
            reducer.reduce(key, values.iterator(), output, reporter);
            
            assertEquals(1, collectedKeys.size());
            assertEquals(op, collectedKeys.get(0).toString());
            String result = collectedValues.get(0).toString();
            assertTrue(result.contains("2"));
            assertTrue(result.contains("300"));
            assertTrue(result.contains("|"));
        }
    }

    @Test
    public void testReduceWithZeroValues() throws IOException {
        Text key = new Text("mkdir");
        List<Text> values = new ArrayList<>();
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        assertEquals(0, collectedKeys.size());
        verify(output, never()).collect(any(Text.class), any(Text.class));
    }

    @Test
    public void testReduceWithInvalidValue() throws IOException {
        Text key = new Text("mkdir");
        List<Text> values = Arrays.asList(new Text("100"), new Text("invalid"), new Text("200"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        assertEquals(1, collectedKeys.size());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("300"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceCalculatesAverage() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(
            new Text("15"),
            new Text("25"),
            new Text("35")
        );
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("25"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceFindsMinAndMax() throws IOException {
        Text key = new Text("read");
        List<Text> values = Arrays.asList(
            new Text("1"),
            new Text("1000"),
            new Text("500")
        );
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("1"));
        assertTrue(result.contains("1000"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceWithAllZeroDurations() throws IOException {
        Text key = new Text("mkdir");
        List<Text> values = Arrays.asList(
            new Text("0"),
            new Text("0"),
            new Text("0")
        );
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("0"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceOutputFormat() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("10"), new Text("20"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        String expectedFormat = "| write      |    2 |         30 |        15 |        10 |        20 |";
        assertEquals(expectedFormat, result);
    }

    @Test
    public void testReduceWithSingleDuration() throws IOException {
        Text key = new Text("delete_dir");
        List<Text> values = Arrays.asList(new Text("5"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("| delete_dir |    1 |"));
        assertTrue(result.contains("|"));
    }

    @Test
    public void testReduceStatusUpdates() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("10"), new Text("20"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        verify(reporter).setStatus(contains("Reducing operation: write"));
        verify(reporter).setStatus(contains("Writing stats for write"));
    }
}
