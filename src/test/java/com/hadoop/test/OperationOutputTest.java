package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

public class OperationOutputTest {

    @Test
    public void testConstructorWithKeyAndValue() {
        OperationOutput output = new OperationOutput("LONG:mkdir*duration", 100L);
        assertEquals(OperationOutput.OutputType.LONG, output.getOutputType());
        assertEquals("mkdir", output.getOperationType());
        assertEquals("duration", output.getMeasurementType());
        assertEquals(100L, output.getValue());
    }

    @Test
    public void testConstructorWithTextAndValue() {
        Text key = new Text("INTEGER:ls*count");
        OperationOutput output = new OperationOutput(key, 5);
        assertEquals(OperationOutput.OutputType.INTEGER, output.getOutputType());
        assertEquals("ls", output.getOperationType());
        assertEquals("count", output.getMeasurementType());
        assertEquals(5, output.getValue());
    }

    @Test
    public void testConstructorWithAllParameters() {
        OperationOutput output = new OperationOutput(OperationOutput.OutputType.DOUBLE, "write", "throughput", 100.5);
        assertEquals(OperationOutput.OutputType.DOUBLE, output.getOutputType());
        assertEquals("write", output.getOperationType());
        assertEquals("throughput", output.getMeasurementType());
        assertEquals(100.5, output.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidKeyFormat() {
        new OperationOutput("invalid_key", 100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidOutputType() {
        new OperationOutput("INVALID_TYPE:operation*measurement", 100);
    }

    @Test
    public void testGetKey() {
        OperationOutput output = new OperationOutput("LONG:mkdir*duration", 100L);
        Text key = output.getKey();
        assertEquals("LONG:mkdir*duration", key.toString());
    }

    @Test
    public void testGetOutputValue() {
        OperationOutput output = new OperationOutput("LONG:mkdir*duration", 100L);
        Text value = output.getOutputValue();
        assertEquals("100", value.toString());
    }

    @Test
    public void testMergeLongValues() {
        OperationOutput o1 = new OperationOutput("LONG:mkdir*duration", 100L);
        OperationOutput o2 = new OperationOutput("LONG:mkdir*duration", 50L);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.LONG, merged.getOutputType());
        assertEquals("mkdir", merged.getOperationType());
        assertEquals("duration", merged.getMeasurementType());
        assertEquals(150L, merged.getValue());
    }

    @Test
    public void testMergeIntegerValues() {
        OperationOutput o1 = new OperationOutput("INTEGER:ls*count", 5);
        OperationOutput o2 = new OperationOutput("INTEGER:ls*count", 3);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.INTEGER, merged.getOutputType());
        assertEquals("ls", merged.getOperationType());
        assertEquals("count", merged.getMeasurementType());
        assertEquals(8, merged.getValue());
    }

    @Test
    public void testMergeFloatValues() {
        OperationOutput o1 = new OperationOutput("FLOAT:write*throughput", 10.5f);
        OperationOutput o2 = new OperationOutput("FLOAT:write*throughput", 20.3f);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.FLOAT, merged.getOutputType());
        assertEquals("write", merged.getOperationType());
        assertEquals("throughput", merged.getMeasurementType());
        assertEquals(30.8f, (Float) merged.getValue(), 0.01f);
    }

    @Test
    public void testMergeDoubleValues() {
        OperationOutput o1 = new OperationOutput("DOUBLE:read*bytes", 1000.0);
        OperationOutput o2 = new OperationOutput("DOUBLE:read*bytes", 500.0);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.DOUBLE, merged.getOutputType());
        assertEquals("read", merged.getOperationType());
        assertEquals("bytes", merged.getMeasurementType());
        assertEquals(1500.0, (Double) merged.getValue(), 0.01);
    }

    @Test
    public void testMergeStringValues() {
        OperationOutput o1 = new OperationOutput("STRING:error*message", "error1");
        OperationOutput o2 = new OperationOutput("STRING:error*message", "error2");
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.STRING, merged.getOutputType());
        assertEquals("error", merged.getOperationType());
        assertEquals("message", merged.getMeasurementType());
        assertEquals("error1;error2", merged.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeDifferentOperationTypes() {
        OperationOutput o1 = new OperationOutput("LONG:mkdir*duration", 100L);
        OperationOutput o2 = new OperationOutput("LONG:write*duration", 50L);
        OperationOutput.merge(o1, o2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeDifferentMeasurementTypes() {
        OperationOutput o1 = new OperationOutput("LONG:mkdir*duration", 100L);
        OperationOutput o2 = new OperationOutput("LONG:mkdir*count", 50L);
        OperationOutput.merge(o1, o2);
    }

    @Test
    public void testMergeLongAndInteger() {
        OperationOutput o1 = new OperationOutput("LONG:mkdir*duration", 100L);
        OperationOutput o2 = new OperationOutput("INTEGER:mkdir*duration", 50);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.LONG, merged.getOutputType());
        assertEquals(150L, merged.getValue());
    }

    @Test
    public void testMergeDoubleAndFloat() {
        OperationOutput o1 = new OperationOutput("DOUBLE:write*bytes", 1000.0);
        OperationOutput o2 = new OperationOutput("FLOAT:write*bytes", 500.0f);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.DOUBLE, merged.getOutputType());
        assertEquals(1500.0, merged.getValue());
    }

    @Test
    public void testMergeStringAndLong() {
        OperationOutput o1 = new OperationOutput("STRING:error*message", "error1");
        OperationOutput o2 = new OperationOutput("STRING:error*message", 100L);
        OperationOutput merged = OperationOutput.merge(o1, o2);
        assertEquals(OperationOutput.OutputType.STRING, merged.getOutputType());
        assertEquals("error1;100", merged.getValue());
    }

    @Test
    public void testToString() {
        OperationOutput output = new OperationOutput("LONG:mkdir*duration", 100L);
        String str = output.toString();
        assertEquals("LONG:mkdir*duration (100)", str);
    }

    @Test
    public void testGetKeyString() {
        OperationOutput output = new OperationOutput("INTEGER:ls*count", 5);
        Text key = output.getKey();
        assertEquals("INTEGER:ls*count", key.toString());
    }

    @Test
    public void testGetOutputValueText() {
        OperationOutput output = new OperationOutput("LONG:mkdir*duration", 100L);
        Text value = output.getOutputValue();
        assertEquals("100", value.toString());
    }

    @Test
    public void testAllOutputTypes() {
        OperationOutput stringOutput = new OperationOutput("STRING:op*meas", "value");
        assertEquals(OperationOutput.OutputType.STRING, stringOutput.getOutputType());

        OperationOutput floatOutput = new OperationOutput("FLOAT:op*meas", 1.0f);
        assertEquals(OperationOutput.OutputType.FLOAT, floatOutput.getOutputType());

        OperationOutput longOutput = new OperationOutput("LONG:op*meas", 1L);
        assertEquals(OperationOutput.OutputType.LONG, longOutput.getOutputType());

        OperationOutput doubleOutput = new OperationOutput("DOUBLE:op*meas", 1.0);
        assertEquals(OperationOutput.OutputType.DOUBLE, doubleOutput.getOutputType());

        OperationOutput intOutput = new OperationOutput("INTEGER:op*meas", 1);
        assertEquals(OperationOutput.OutputType.INTEGER, intOutput.getOutputType());
    }
}
