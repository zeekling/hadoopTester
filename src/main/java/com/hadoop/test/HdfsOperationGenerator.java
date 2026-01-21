package com.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsOperationGenerator implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsOperationGenerator.class);

    private Configuration base;

    public HdfsOperationGenerator(Configuration base) {
        this.base = base;
    }

    public static void main(String[] args) throws Exception {
        Configuration startCfg = new Configuration(true);
        HdfsOperationGenerator runner = new HdfsOperationGenerator(startCfg);
        int ec = ToolRunner.run(runner, args);
        System.exit(ec);
    }

    @Override
    public int run(String[] args) throws Exception {
        ArgumentParser.ParsedOutput parsedOpts = null;
        try {
            ArgumentParser argHolder = new ArgumentParser(args);
            parsedOpts = argHolder.parse();
            if (parsedOpts.shouldOutputHelp()) {
                parsedOpts.outputHelp();
                return 1;
            }
        } catch (Exception e) {
            LOG.error("Unable to parse arguments due to error: ", e);
            return 1;
        }

        String operations = parsedOpts.getValue(ConfigOption.OPERATIONS.getOpt());
        int numMaps = parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), ConfigOption.MAPS.getDefaultValue());

        System.out.println("Generating HDFS operation info...");
        System.out.println("operations=" + operations);
        System.out.println("numMaps=" + numMaps);

        JobConf job = getJob(parsedOpts);
        JobClient.runJob(job);
        return 0;
    }

    private JobConf getJob(ArgumentParser.ParsedOutput opts) {
        JobConf job = new JobConf(base, HdfsOperationGenerator.class);
        job.setInputFormat(DummyInputFormat.class);
        FileOutputFormat.setOutputPath(job, opts.getInputPath());
        job.setMapperClass(OperationGeneratorMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, false);
        job.setNumMapTasks(opts.getValueAsInt(ConfigOption.MAPS.getOpt(), ConfigOption.MAPS.getDefaultValue()));

        job.set(ConfigOption.OPERATIONS.getCfgOption(), opts.getValue(ConfigOption.OPERATIONS.getOpt(), ConfigOption.OPERATIONS.getDefaultValue()));
        job.set(ConfigOption.OPS_PER_MAPPER.getCfgOption(), String.valueOf(opts.getValueAsInt(ConfigOption.OPS_PER_MAPPER.getOpt(), ConfigOption.OPS_PER_MAPPER.getDefaultValue())));
        job.set("mapred.job.map.memory.mb", opts.getValue(ConfigOption.MAP_MEMORY_MB.getOpt(), ConfigOption.MAP_MEMORY_MB.getDefaultValue()));

        return job;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

}
