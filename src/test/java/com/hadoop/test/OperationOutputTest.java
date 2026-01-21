package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

public class OperationOutputTest {

    @Test
    public void testConstructorWithKeyAndValue() {
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
        assertEquals(OperationOutput.OutputType.LONG, output.getOutputType());
        assertEquals("write", output.getOperationType());
        assertEquals("duration", output.getMeasurementType());
        assertEquals(100L, output.getValue());
        assertEquals(1L, output.getCount());
    }

    @Test
    public void testConstructorWithTextAndValue() {
        Text key = new Text("INTEGER:read*count");
        OperationOutput output = new OperationOutput(key, 5);
        assertEquals(OperationOutput.OutputType.INTEGER, output.getOutputType());
        assertEquals("read", output.getOperationType());
        assertEquals("count", output.getMeasurementType());
        assertEquals(5, output.getValue());
        assertEquals(1L, output.getCount());
    }

    @Test
    public void testConstructorWithAllParameters() {
        OperationOutput output = new OperationOutput(OperationOutput.OutputType.DOUBLE, "write", "throughput", 100.5, 5);
        assertEquals(OperationOutput.OutputType.DOUBLE, output.getOutputType());
        assertEquals("write", output.getOperationType());
        assertEquals("throughput", output.getMeasurementType());
        assertEquals(100.5, output.getValue());
        assertEquals(5L, output.getCount());
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
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
        Text key = output.getKey();
        assertEquals("LONG:write*duration", key.toString());
    }

    @Test
    public void testGetOutputValue() {
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
        Text value = output.getOutputValue();
        assertEquals("100", value.toString());
    }

    @Test
    public void testToString() {
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
        String str = output.toString();
        assertEquals("LONG:write*duration (100)", str);
    }

    @Test
    public void testGetKeyString() {
        OperationOutput output = new OperationOutput("INTEGER:read*count", 5);
        Text key = output.getKey();
        assertEquals("INTEGER:read*count", key.toString());
    }

    @Test
    public void testGetOutputValueText() {
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
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

    @Test
    public void testDefaultCount() {
        OperationOutput output = new OperationOutput("LONG:write*duration", 100L);
        assertEquals(1L, output.getCount());
    }

    @Test
    public void testCustomCount() {
        OperationOutput output = new OperationOutput(OperationOutput.OutputType.LONG, "write", "duration", 100L, 5);
        assertEquals(5L, output.getCount());
        assertEquals(100L, output.getValue());
    }
}
