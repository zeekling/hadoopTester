package com.hadoop.test;


import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        String baseDir = parsedOpts.getValue(ConfigOption.BASE_DIR.getOpt());
        System.out.println("baseDir=" + baseDir);
        String operations = parsedOpts.getValue(ConfigOption.OPERATIONS.getOpt());
        System.out.println("operations=" + operations);
        int fileSize = parsedOpts.getValueAsInt(ConfigOption.FILE_SIZE.getOpt(), ConfigOption.FILE_SIZE.getDefaultValue());
        System.out.println("fileSize=" + fileSize + " MB");
        int opsPerMapper = parsedOpts.getValueAsInt(ConfigOption.OPS_PER_MAPPER.getOpt(), ConfigOption.OPS_PER_MAPPER.getDefaultValue());
        System.out.println("opsPerMapper=" + opsPerMapper);
        int numMaps = parsedOpts.getValueAsInt(ConfigOption.MAPS.getOpt(), ConfigOption.MAPS.getDefaultValue());
        System.out.println("numMaps=" + numMaps);
        JobConf job = getJob(parsedOpts);
        JobClient.runJob(job);
        displayResults(job);
        return 0;
    }

    private JobConf getJob(ArgumentParser.ParsedOutput opts) {
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
        job.setNumReduceTasks(1);
        job.setNumMapTasks(opts.getValueAsInt(ConfigOption.MAPS.getOpt(), ConfigOption.MAPS.getDefaultValue()));
        
        job.set(ConfigOption.BASE_DIR.getCfgOption(), opts.getValue(ConfigOption.BASE_DIR.getOpt(), ConfigOption.BASE_DIR.getDefaultValue()));
        job.set(ConfigOption.OPERATIONS.getCfgOption(), opts.getValue(ConfigOption.OPERATIONS.getOpt(), ConfigOption.OPERATIONS.getDefaultValue()));
        job.setInt(ConfigOption.FILE_SIZE.getCfgOption(), opts.getValueAsInt(ConfigOption.FILE_SIZE.getOpt(), ConfigOption.FILE_SIZE.getDefaultValue()));
        job.setInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), opts.getValueAsInt(ConfigOption.OPS_PER_MAPPER.getOpt(), ConfigOption.OPS_PER_MAPPER.getDefaultValue()));
        job.setInt(ConfigOption.THREAD_POOL_SIZE.getCfgOption(), opts.getValueAsInt(ConfigOption.THREAD_POOL_SIZE.getOpt(), ConfigOption.THREAD_POOL_SIZE.getDefaultValue()));

        return job;
    }


    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    private void displayResults(JobConf job) {
        try {
            Path outputPath = FileOutputFormat.getOutputPath(job);
            FileSystem fs = outputPath.getFileSystem(job);

            Path resultPath = new Path(outputPath, "part-00000");
            if (!fs.exists(resultPath)) {
                LOG.warn("Output file not found: " + resultPath);
                return;
            }

            System.out.println("\n========== Test Results ==========");
            String header = "+------------+------+------------+-----------+-----------+-----------+";
            String title = "| Operation  | Count| Total(ms)  | Avg(ms)   | Min(ms)   | Max(ms)   |";
            String divider = "+------------+------+------------+-----------+-----------+-----------+";
            
            System.out.println(header);
            System.out.println(title);
            System.out.println(divider);
            
            org.apache.hadoop.fs.FSDataInputStream in = fs.open(resultPath);
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
            in.close();
            
            System.out.println(divider);
            System.out.println("===================================\n");
        } catch (IOException e) {
            LOG.error("Error reading results: ", e);
        }
    }
}
