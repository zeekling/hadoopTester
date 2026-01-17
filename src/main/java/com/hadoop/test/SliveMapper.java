/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hadoop.test;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The slive class which sets up the mapper to be used which itself will receive
 * a single dummy key and value and then in a loop run the various operations
 * that have been selected and upon operation completion output to be collected
 * output from that operation (and repeat until finished).
 */
public class SliveMapper extends MapReduceBase implements
        Mapper<Object, Object, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(SliveMapper.class);

    private static final String OP_TYPE = SliveMapper.class.getSimpleName();

    private FileSystem filesystem;
    private int taskId;
    private HdfsOperation operation;
    private String[] operations;
    private int opsPerMapper;

    @Override
    public void configure(JobConf conf) {
        try {
            String baseDir = conf.get(ConfigOption.BASE_DIR.getCfgOption(), ConfigOption.BASE_DIR.getDefaultValue());
            filesystem = new Path(baseDir).getFileSystem(conf);

            int fileSize = conf.getInt(ConfigOption.FILE_SIZE.getCfgOption(), ConfigOption.FILE_SIZE.getDefaultValue());

            if (conf.get(MRJobConfig.TASK_ATTEMPT_ID) != null) {
                this.taskId = TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID))
                        .getTaskID().getId();
            } else {
                this.taskId = TaskAttemptID.forName(conf.get("mapred.task.id"))
                        .getTaskID().getId();
            }

            int threadPoolSize = conf.getInt(ConfigOption.THREAD_POOL_SIZE.getCfgOption(), ConfigOption.THREAD_POOL_SIZE.getDefaultValue());
            operation = new HdfsOperation(filesystem, baseDir, taskId, fileSize, threadPoolSize);

            String opsStr = conf.get(ConfigOption.OPERATIONS.getCfgOption(), ConfigOption.OPERATIONS.getDefaultValue());
            operations = opsStr.split(",");

            opsPerMapper = conf.getInt(ConfigOption.OPS_PER_MAPPER.getCfgOption(), ConfigOption.OPS_PER_MAPPER.getDefaultValue());

        } catch (Exception e) {
            LOG.error("Unable to setup slive " + StringUtils.stringifyException(e));
            throw new RuntimeException("Unable to setup slive configuration", e);
        }
    }

    private void logAndSetStatus(Reporter r, String msg) {
        r.setStatus(msg);
        LOG.info(msg);
    }

    @Override
    public void map(Object key, Object value, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        logAndSetStatus(reporter, "Running slive mapper for dummy key " + key
                + " and dummy value " + value);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int[] completedOps = {0};

        int opIndex = 0;
        for (int i = 0; i < opsPerMapper; i++) {
            String opType = operations[opIndex % operations.length];
            final int currentIndex = i;

            CompletableFuture<Void> future = operation.executeAsync(opType, currentIndex)
                .thenAccept(result -> {
                    try {
                        long duration = Long.parseLong(result.getValue().toString());
                        String opKey = opType;
                        String opValue = String.valueOf(duration);
                        output.collect(new Text(opKey), new Text(opValue));

                        synchronized (completedOps) {
                            completedOps[0]++;
                            if (completedOps[0] % 100 == 0) {
                                logAndSetStatus(reporter, "Completed " + completedOps[0] + " operations");
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error processing result for operation " + opType + " at index " + currentIndex, e);
                    }
                });

            futures.add(future);
            opIndex++;
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        operation.shutdown();

        logAndSetStatus(reporter, "Completed all " + opsPerMapper + " operations");
    }
}
