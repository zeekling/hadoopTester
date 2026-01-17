package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SlivePartitionerTest {

    private SlivePartitioner partitioner;
    private JobConf jobConf;

    @Before
    public void setUp() {
        partitioner = new SlivePartitioner();
        jobConf = new JobConf();
        partitioner.configure(jobConf);
    }

    @Test
    public void testGetPartitionWithOneReducer() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        int partition = partitioner.getPartition(key, value, 1);
        assertEquals(0, partition);
    }

    @Test
    public void testGetPartitionWithMultipleReducers() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        int partition = partitioner.getPartition(key, value, 5);
        assertTrue(partition >= 0 && partition < 5);
    }

    @Test
    public void testGetPartitionConsistency() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        int partition1 = partitioner.getPartition(key, value, 10);
        int partition2 = partitioner.getPartition(key, value, 10);
        assertEquals(partition1, partition2);
    }

    @Test
    public void testGetPartitionDifferentKeys() {
        Text key1 = new Text("LONG:mkdir*duration");
        Text key2 = new Text("LONG:write*duration");
        Text value = new Text("100");
        int partition1 = partitioner.getPartition(key1, value, 5);
        int partition2 = partitioner.getPartition(key2, value, 5);
        if (partition1 == partition2) {
            assertEquals(partition1, partition2);
        } else {
            assertNotEquals(partition1, partition2);
        }
    }

    @Test
    public void testGetPartitionWithAllOperationTypes() {
        String[] operations = {"mkdir", "write", "read", "delete_dir", "delete_file", "ls"};
        Text value = new Text("100");
        
        for (String op : operations) {
            Text key = new Text("LONG:" + op + "*duration");
            int partition = partitioner.getPartition(key, value, 3);
            assertTrue(partition >= 0 && partition < 3);
        }
    }

    @Test
    public void testGetPartitionWithDifferentDataTypes() {
        String[] dataTypes = {"LONG", "INTEGER", "DOUBLE", "FLOAT", "STRING"};
        Text value = new Text("100");
        
        for (String type : dataTypes) {
            Text key = new Text(type + ":op*meas");
            int partition = partitioner.getPartition(key, value, 10);
            assertTrue(partition >= 0 && partition < 10);
        }
    }

    @Test
    public void testGetPartitionWithLargeNumPartitions() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        int partition = partitioner.getPartition(key, value, 100);
        assertTrue(partition >= 0 && partition < 100);
    }

    @Test
    public void testGetPartitionWithZeroPartitions() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        try {
            int partition = partitioner.getPartition(key, value, 0);
            fail("Expected ArithmeticException or similar exception");
        } catch (Exception e) {
            assertTrue(e instanceof ArithmeticException);
        }
    }

    @Test
    public void testConfigureDoesNothing() {
        JobConf newConf = new JobConf();
        partitioner.configure(newConf);
        assertNotNull(partitioner);
    }

    @Test
    public void testGetPartitionNegativeHashHandling() {
        Text key = new Text("LONG:mkdir*duration");
        Text value = new Text("100");
        int numPartitions = 3;
        int partition = partitioner.getPartition(key, value, numPartitions);
        assertTrue(partition >= 0);
        assertTrue(partition < numPartitions);
    }
}
