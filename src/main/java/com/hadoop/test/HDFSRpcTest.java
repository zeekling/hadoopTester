package com.hadoop.test;


import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HDFSRpcTest implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSRpcTest.class);

    private Configuration base;

    public HDFSRpcTest(Configuration base) {
        this.base = base;
    }


    public static void main(String[] args) throws Exception {
        Configuration startCfg = new Configuration(true);
        HDFSRpcTest runner = new HDFSRpcTest(startCfg);
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
        String value = parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt());
        System.out.println("baseDir=" + value);
        List<String> argList = parsedOpts.getParsedData().getArgList();
        for (String arg : argList) {
            System.out.println("arg: " + arg);
        }
        JobClient.runJob(getJob(parsedOpts));
        return 0;
    }

    private JobConf getJob(ArgumentParser.ParsedOutput opts) throws IOException, ParseException {
        JobConf job = new JobConf(base, HDFSRpcTest.class);
        job.setInputFormat(DummyInputFormat.class);
        FileOutputFormat.setOutputPath(job, opts.getOutputPath());
        job.setMapperClass(SliveMapper.class);
        job.setPartitionerClass(SlivePartitioner.class);
        job.setReducerClass(SliveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, false);
        job.setNumReduceTasks(opts.getValueAsInt(ConfigOption.REDUCES.getOpt(), 1));
        job.setNumMapTasks(opts.getValueAsInt(ConfigOption.MAPS.getOpt(), 2));
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
