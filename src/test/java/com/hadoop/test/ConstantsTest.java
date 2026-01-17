package com.hadoop.test;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConstantsTest {

    @Test
    public void testProgName() {
        assertEquals("HDFSRpcTest", Constants.PROG_NAME);
    }

    @Test
    public void testProgVersion() {
        assertEquals("0.1.0", Constants.PROG_VERSION);
    }
}
