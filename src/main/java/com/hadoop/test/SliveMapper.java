/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

/**
 * The slive class which sets up the mapper to be used which itself will receive
 * a single dummy key and value and then in a loop run the various operations
 * that have been selected and upon operation completion output the collected
 * output from that operation (and repeat until finished).
 */
public class SliveMapper extends MapReduceBase implements
        Mapper<Object, Object, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(SliveMapper.class);

    private static final String OP_TYPE = SliveMapper.class.getSimpleName();

    private FileSystem filesystem;
    private int taskId;

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
     * .JobConf)
     */
    @Override // MapReduceBase
    public void configure(JobConf conf) {
        try {
            filesystem = new Path(conf.get(ConfigOption.BASE_DIR.getCfgOption())).getFileSystem(conf);
        } catch (Exception e) {
            LOG.error("Unable to setup slive " + StringUtils.stringifyException(e));
            throw new RuntimeException("Unable to setup slive configuration", e);
        }
        if (conf.get(MRJobConfig.TASK_ATTEMPT_ID) != null) {
            this.taskId = TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID))
                    .getTaskID().getId();
        } else {
            // So that branch-1/0.20 can run this same code as well
            this.taskId = TaskAttemptID.forName(conf.get("mapred.task.id"))
                    .getTaskID().getId();
        }
    }


    /**
     * Logs to the given reporter and logs to the internal logger at info level
     *
     * @param r
     *          the reporter to set status on
     * @param msg
     *          the message to log
     */
    private void logAndSetStatus(Reporter r, String msg) {
        r.setStatus(msg);
        LOG.info(msg);
    }


    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
     * org.apache.hadoop.mapred.Reporter)
     */
    @Override // Mapper
    public void map(Object key, Object value, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        logAndSetStatus(reporter, "Running slive mapper for dummy key " + key
                + " and dummy value " + value);

    }
}
