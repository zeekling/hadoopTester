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
        assertTrue(collectedValues.get(0).toString().contains("count=1"));
        assertTrue(collectedValues.get(0).toString().contains("total=100ms"));
        assertTrue(collectedValues.get(0).toString().contains("avg=100ms"));
        assertTrue(collectedValues.get(0).toString().contains("min=100ms"));
        assertTrue(collectedValues.get(0).toString().contains("max=100ms"));
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
        assertTrue(collectedValues.get(0).toString().contains("count=3"));
        assertTrue(collectedValues.get(0).toString().contains("total=60ms"));
        assertTrue(collectedValues.get(0).toString().contains("avg=20ms"));
        assertTrue(collectedValues.get(0).toString().contains("min=10ms"));
        assertTrue(collectedValues.get(0).toString().contains("max=30ms"));
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
        
        assertTrue(collectedValues.get(0).toString().contains("total=800ms"));
        assertTrue(collectedValues.get(0).toString().contains("avg=160ms"));
        assertTrue(collectedValues.get(0).toString().contains("min=50ms"));
        assertTrue(collectedValues.get(0).toString().contains("max=300ms"));
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
            assertTrue(collectedValues.get(0).toString().contains("count=2"));
            assertTrue(collectedValues.get(0).toString().contains("total=300ms"));
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
        assertTrue(collectedValues.get(0).toString().contains("count=2"));
        assertTrue(collectedValues.get(0).toString().contains("total=300ms"));
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
        assertTrue(result.contains("avg=25ms"));
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
        assertTrue(result.contains("min=1ms"));
        assertTrue(result.contains("max=1000ms"));
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
        assertTrue(result.contains("total=0ms"));
        assertTrue(result.contains("avg=0ms"));
        assertTrue(result.contains("min=0ms"));
        assertTrue(result.contains("max=0ms"));
    }

    @Test
    public void testReduceOutputFormat() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("10"), new Text("20"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        String expectedFormat = "count=2, total=30ms, avg=15ms, min=10ms, max=20ms";
        assertEquals(expectedFormat, result);
    }

    @Test
    public void testReduceWithSingleDuration() throws IOException {
        Text key = new Text("delete_dir");
        List<Text> values = Arrays.asList(new Text("5"));
        
        reducer.reduce(key, values.iterator(), output, reporter);
        
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("count=1, total=5ms, avg=5ms, min=5ms, max=5ms"));
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
