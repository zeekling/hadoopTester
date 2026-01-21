package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class OperationGeneratorMapper extends MapReduceBase implements Mapper<Object, Object, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(OperationGeneratorMapper.class);
    private static final Random RANDOM = new Random();

    private int taskId;
    private String[] operations;
    private int opsPerMapper;

    @Override
    public void configure(JobConf conf) {
        try {
            if (conf.get(MRJobConfig.TASK_ATTEMPT_ID) != null) {
                this.taskId = TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID))
                        .getTaskID().getId();
            } else {
                this.taskId = TaskAttemptID.forName(conf.get("mapred.task.id"))
                        .getTaskID().getId();
            }

            String opsStr = conf.get(ConfigOption.OPERATIONS.getCfgOption(), ConfigOption.OPERATIONS.getDefaultValue());
            operations = opsStr.split(",");

            opsPerMapper = conf.getInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), ConfigOption.OPS_PER_MAPPER.getDefaultValue());

        } catch (Exception e) {
            LOG.error("Unable to setup operation generator " + StringUtils.stringifyException(e));
            throw new RuntimeException("Unable to setup operation generator configuration", e);
        }
    }

    private void logAndSetStatus(Reporter r, String msg) {
        r.setStatus(msg);
        LOG.info(msg);
    }

    @Override
    public void map(Object key, Object value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        logAndSetStatus(reporter, "Running operation generator for mapper " + taskId);

        int generatedCount = 0;
        int opIndex = 0;

        for (int i = 0; i < opsPerMapper; i++) {
            String opType = operations[opIndex % operations.length];
            String randomKey = generateRandomKey();

            String outputValue = opType + ":" + randomKey;
            output.collect(null, new Text(outputValue));

            generatedCount++;
            if (generatedCount % 100 == 0) {
                logAndSetStatus(reporter, "Generated " + generatedCount + " operation entries");
            }

            opIndex++;
        }

        logAndSetStatus(reporter, "Completed generating " + generatedCount + " operation entries");
    }

    private String generateRandomKey() {
        return UUID.randomUUID().toString();
    }
}
