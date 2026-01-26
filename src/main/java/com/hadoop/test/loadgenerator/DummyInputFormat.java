package com.hadoop.test.loadgenerator;

import java.io.IOException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configured;

public class DummyInputFormat extends Configured implements InputFormat<LongWritable, Text> {
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) {
    int splits = conf.getInt("LG.numMapTasks", 1);
    InputSplit[] ret = new InputSplit[splits];
    for (int i = 0; i < splits; ++i) {
      ret[i] = new EmptySplit();
    }
    return ret;
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit ignored, JobConf conf, Reporter reporter) throws IOException {
    return new com.hadoop.test.loadgenerator.DummySingleRecordReader();
  }
}
