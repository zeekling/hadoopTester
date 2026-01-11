package com.hadoop.test;

import lombok.Getter;
import org.apache.commons.cli.Option;

public class ConfigOption<T> extends Option {

    private static final String PREFIX = "test";

    // command line options and descriptions and config option name
    static final ConfigOption<Integer> MAPS = new ConfigOption<Integer>(
            "maps", true, "Number of maps", PREFIX + ".maps", 10);

    static final ConfigOption<Integer> REDUCES = new ConfigOption<Integer>(
            "reduces", true, "Number of reduces", PREFIX + ".reduces", 1);

    static final ConfigOption<String> BASE_DIR = new ConfigOption<String>("baseDir", true, "Base directory path", PREFIX + ".base.dir",
            "/test/hdfsrpc");

    static final Option HELP = new Option("help", false, "Usage information");


    /**
     * Hadoop configuration property name
     */
    @Getter
    private String cfgOption;

    /**
     * Default value if no value is located by other means
     */
    @Getter
    private T defaultValue;

    ConfigOption(String cliOption, boolean hasArg, String description,
                 String cfgOption, T def) {
        super(cliOption, hasArg, description);
        this.cfgOption = cfgOption;
        this.defaultValue = def;
    }

}
