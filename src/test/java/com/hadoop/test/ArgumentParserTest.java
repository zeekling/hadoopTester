package com.hadoop.test;

import org.junit.Test;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.*;

public class ArgumentParserTest {

    @Test
    public void testParseWithHelp() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-h"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertTrue(parsedOpts.shouldOutputHelp());
    }

    @Test
    public void testParseWithHelpLong() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"--help"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertTrue(parsedOpts.shouldOutputHelp());
    }

    @Test
    public void testParseWithoutHelp() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "5"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertFalse(parsedOpts.shouldOutputHelp());
    }

    @Test
    public void testParseWithNullArgs() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(null);
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertFalse(parsedOpts.shouldOutputHelp());
    }

    @Test
    public void testParseWithEmptyArgs() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertFalse(parsedOpts.shouldOutputHelp());
    }

    @Test
    public void testGetValueWithValidOption() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "5", "-baseDir", "/test/dir"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals("5", parsedOpts.getValue(ConfigOption.MAPS.getOpt()));
        assertEquals("/test/dir", parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt()));
    }

    @Test
    public void testGetValueWithMissingOption() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "5"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertNull(parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt()));
    }

    @Test
    public void testGetValueWithDefaultValue() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "5"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals("/custom/dir", parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt(), "/custom/dir"));
    }

    @Test
    public void testGetValueAsIntWithValidOption() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "5"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals(5, parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), 10));
    }

    @Test
    public void testGetValueAsIntWithDefaultValue() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-baseDir", "/test"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals(10, parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), 10));
    }

    @Test
    public void testGetOutputPath() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-baseDir", "/test/dir"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        Path path = parsedOpts.getOutputPath();
        assertEquals(new Path("/test/dir/output"), path);
    }

    @Test
    public void testGetOutputPathWithDefault() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-baseDir", "/test/dir"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        Path path = parsedOpts.getOutputPath();
        assertEquals(new Path("/test/dir/output"), path);
    }

    @Test
    public void testParseOperations() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-operations", "write,read,delete_file"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals("write,read,delete_file", parsedOpts.getValue(ConfigOption.OPERATIONS.getOpt()));
    }

    @Test
    public void testParseMultipleOptions() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{
            "-maps", "5",
            "-baseDir", "/test/dir",
            "-operations", "write,read",
            "-fileSize", "20",
            "-fileCount", "50",
            "-opsPerMapper", "500"
        });
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals("5", parsedOpts.getValue(ConfigOption.MAPS.getOpt()));
        assertEquals("/test/dir", parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt()));
        assertEquals("write,read", parsedOpts.getValue(ConfigOption.OPERATIONS.getOpt()));
        assertEquals("20", parsedOpts.getValue(ConfigOption.FILE_SIZE.getOpt()));
        assertEquals("50", parsedOpts.getValue(ConfigOption.FILE_COUNT.getOpt()));
        assertEquals("500", parsedOpts.getValue(ConfigOption.OPS_PER_MAPPER.getOpt()));
    }

    @Test
    public void testGetValueAsIntForMultipleOptions() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{
            "-maps", "5",
            "-fileSize", "20"
        });
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        assertEquals(5, parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), 10));
        assertEquals(20, parsedOpts.getValueAsInt(ConfigOption.FILE_SIZE.getOpt(), 10));
        assertEquals(10, parsedOpts.getValueAsInt(ConfigOption.FILE_COUNT.getOpt(), 10));
    }

    @Test
    public void testGetValueAsIntWithInvalidNumber() throws Exception {
        ArgumentParser argHolder = new ArgumentParser(new String[]{"-maps", "invalid"});
        ArgumentParser.ParsedOutput parsedOpts = argHolder.parse();
        try {
            parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), 10);
            fail("Expected NumberFormatException");
        } catch (NumberFormatException e) {
            assertEquals("For input string: \"invalid\"", e.getMessage());
        }
    }
}
