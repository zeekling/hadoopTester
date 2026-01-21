package com.hadoop.test;

import lombok.Getter;
import org.apache.commons.cli.Option;

public class ConfigOption<T> extends Option {

    private static final String PREFIX = "test";

    // command line options and descriptions and config option name
    static final ConfigOption<Integer> MAPS = new ConfigOption<Integer>(
            "maps", true, "Number of maps", PREFIX + ".maps", 10);

    static final ConfigOption<String> BASE_DIR = new ConfigOption<String>("baseDir", true, "Base directory path", PREFIX + ".base.dir",
            "/test/hdfsrpc");

    static final ConfigOption<String> OPERATIONS = new ConfigOption<String>("operations", true, "Operations to run (comma separated): write,read,delete_file,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate", PREFIX + ".operations", "write,read,delete_file,rename,get_file_status,exists,set_permission,append,create_symlink,append_truncate");

    static final ConfigOption<Integer> FILE_SIZE = new ConfigOption<Integer>("fileSize", true, "File size in MB", PREFIX + ".file.size", 10);

    static final ConfigOption<Integer> FILE_COUNT = new ConfigOption<Integer>("fileCount", true, "Number of files per operation", PREFIX + ".file.count", 100);

    static final ConfigOption<Integer> OPS_PER_MAPPER = new ConfigOption<Integer>("opsPerMapper", true, "Operations per mapper", PREFIX + ".ops.per.mapper", 10000);

    static final ConfigOption<Integer> THREAD_POOL_SIZE = new ConfigOption<Integer>("threadPoolSize", true, "Thread pool size for async operations", PREFIX + ".thread.pool.size", 10);

    static final ConfigOption<String> MAP_MEMORY_MB = new ConfigOption<String>("mapMemoryMb", true, "Map task memory in MB", PREFIX + ".map.memory.mb", "1024");

    static final ConfigOption<String> REDUCE_MEMORY_MB = new ConfigOption<String>("reduceMemoryMb", true, "Reduce task memory in MB", PREFIX + ".reduce.memory.mb", "512");

    static final Option HELP = new Option("h", "help", false, "Usage information");


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
