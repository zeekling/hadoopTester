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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * The slive reducer which iterates over the given input values and merges them
 * together into a final output value.
 */
public class SliveReducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(SliveReducer.class);


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
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
   * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
   * org.apache.hadoop.mapred.Reporter)
   */
  @Override // Reducer
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    String opType = key.toString();
    long totalTime = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    int count = 0;

    logAndSetStatus(reporter, "Reducing operation: " + opType);

    while (values.hasNext()) {
      Text value = values.next();
      try {
        long duration = Long.parseLong(value.toString());
        totalTime += duration;
        minTime = Math.min(minTime, duration);
        maxTime = Math.max(maxTime, duration);
        count++;
      } catch (Exception e) {
        logAndSetStatus(reporter, "Error parsing duration for " + opType
            + " value: " + value + " due to: " + StringUtils.stringifyException(e));
      }
    }

    if (count > 0) {
      long avgTime = totalTime / count;
      String result = String.format("count=%d, total=%dms, avg=%dms, min=%dms, max=%dms",
          count, totalTime, avgTime, minTime, maxTime);

      logAndSetStatus(reporter, "Writing stats for " + opType + ": " + result);
      output.collect(key, new Text(result));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
   * .JobConf)
   */
  @Override // MapReduceBase
  public void configure(JobConf conf) {

  }

}
