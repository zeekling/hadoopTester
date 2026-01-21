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
        }).when(output).collect(any(), any(Text.class));
    }

    @Test
    public void testConfigure() {
        SliveReducer newReducer = new SliveReducer();
        newReducer.configure(new JobConf());
        assertNotNull(newReducer);
    }

    @Test
    public void testReduceWithSingleValue() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("duration:100"));

        reducer.reduce(key, values.iterator(), output, reporter);

        assertEquals(1, collectedValues.size());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=write"));
        assertTrue(result.contains("Count=1"));
        assertTrue(result.contains("totalTime=100"));
        assertTrue(result.contains("avgTime=100"));
        assertTrue(result.contains("minTime=100"));
        assertTrue(result.contains("maxTime=100"));
        assertTrue(result.contains("errorCount=0"));
    }

    @Test
    public void testReduceWithMultipleValues() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(
            new Text("duration:10"),
            new Text("duration:20"),
            new Text("duration:30")
        );

        reducer.reduce(key, values.iterator(), output, reporter);

        assertEquals(1, collectedValues.size());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=write"));
        assertTrue(result.contains("Count=3"));
        assertTrue(result.contains("totalTime=60"));
        assertTrue(result.contains("avgTime=20"));
        assertTrue(result.contains("minTime=10"));
        assertTrue(result.contains("maxTime=30"));
    }

    @Test
    public void testReduceWithLargeValues() throws IOException {
        Text key = new Text("read");
        List<Text> values = Arrays.asList(
            new Text("duration:100"),
            new Text("duration:200"),
            new Text("duration:150"),
            new Text("duration:50"),
            new Text("duration:300")
        );

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=read"));
        assertTrue(result.contains("Count=5"));
        assertTrue(result.contains("totalTime=800"));
        assertTrue(result.contains("avgTime=160"));
        assertTrue(result.contains("minTime=50"));
        assertTrue(result.contains("maxTime=300"));
    }

    @Test
    public void testReduceWithDifferentOperations() throws IOException {
        String[] operations = {"write", "read", "delete_file", "rename", "get_file_status", "exists"};

        for (String op : operations) {
            collectedKeys.clear();
            collectedValues.clear();

            Text key = new Text(op);
            List<Text> values = Arrays.asList(new Text("duration:100"), new Text("duration:200"));

            reducer.reduce(key, values.iterator(), output, reporter);

            assertEquals(1, collectedValues.size());
            String result = collectedValues.get(0).toString();
            assertTrue(result.contains("Operation=" + op));
            assertTrue(result.contains("Count=2"));
            assertTrue(result.contains("totalTime=300"));
        }
    }

    @Test
    public void testReduceWithZeroValues() throws IOException {
        Text key = new Text("write");
        List<Text> values = new ArrayList<>();

        reducer.reduce(key, values.iterator(), output, reporter);

        assertEquals(0, collectedKeys.size());
        verify(output, never()).collect(any(Text.class), any(Text.class));
    }

    @Test
    public void testReduceWithInvalidValue() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("duration:100"), new Text("invalid"), new Text("duration:200"));

        reducer.reduce(key, values.iterator(), output, reporter);

        assertEquals(1, collectedValues.size());
        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=write"));
        assertTrue(result.contains("Count=2"));
        assertTrue(result.contains("totalTime=300"));
    }

    @Test
    public void testReduceCalculatesAverage() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(
            new Text("duration:15"),
            new Text("duration:25"),
            new Text("duration:35")
        );

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=write"));
        assertTrue(result.contains("Count=3"));
        assertTrue(result.contains("totalTime=75"));
        assertTrue(result.contains("avgTime=25"));
    }

    @Test
    public void testReduceFindsMinAndMax() throws IOException {
        Text key = new Text("read");
        List<Text> values = Arrays.asList(
            new Text("duration:1"),
            new Text("duration:1000"),
            new Text("duration:500")
        );

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=read"));
        assertTrue(result.contains("Count=3"));
        assertTrue(result.contains("totalTime=1501"));
        assertTrue(result.contains("minTime=1"));
        assertTrue(result.contains("maxTime=1000"));
    }

    @Test
    public void testReduceWithAllZeroDurations() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(
            new Text("duration:0"),
            new Text("duration:0"),
            new Text("duration:0")
        );

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=write"));
        assertTrue(result.contains("Count=3"));
        assertTrue(result.contains("totalTime=0"));
        assertTrue(result.contains("avgTime=0"));
        assertTrue(result.contains("minTime=0"));
        assertTrue(result.contains("maxTime=0"));
    }

    @Test
    public void testReduceOutputFormat() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("duration:10"), new Text("duration:20"));

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        String expectedFormat = "Operation=write, Count=2, errorCount=0, totalTime=30, avgTime=15, minTime=10, maxTime=20";
        assertEquals(expectedFormat, result);
    }

    @Test
    public void testReduceWithSingleDuration() throws IOException {
        Text key = new Text("delete_file");
        List<Text> values = Arrays.asList(new Text("duration:5"));

        reducer.reduce(key, values.iterator(), output, reporter);

        String result = collectedValues.get(0).toString();
        assertTrue(result.contains("Operation=delete_file"));
        assertTrue(result.contains("Count=1"));
        assertTrue(result.contains("totalTime=5"));
        assertTrue(result.contains("avgTime=5"));
        assertTrue(result.contains("minTime=5"));
        assertTrue(result.contains("maxTime=5"));
    }

    @Test
    public void testReduceStatusUpdates() throws IOException {
        Text key = new Text("write");
        List<Text> values = Arrays.asList(new Text("duration:10"), new Text("duration:20"));

        reducer.reduce(key, values.iterator(), output, reporter);

        verify(reporter).setStatus(contains("Reducing operation: write"));
        verify(reporter, atLeast(1)).setStatus(anyString());
    }
}
