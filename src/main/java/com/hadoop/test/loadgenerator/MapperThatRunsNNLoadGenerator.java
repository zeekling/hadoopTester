package com.hadoop.test.loadgenerator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class MapperThatRunsNNLoadGenerator extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  private JobConf jobConf;

  @Override
  public void configure(org.apache.hadoop.mapred.JobConf job) {
    this.jobConf = job;
  }

  @Override
  public void map(LongWritable key, Text value,
                  OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    com.hadoop.test.loadgenerator.ProgressThread progressThread =
        new com.hadoop.test.loadgenerator.ProgressThread(reporter);
    progressThread.start();
    try {
      LoadGenerator loader = new LoadGenerator(jobConf);
      loader.generateLoadOnNN();
      System.out.println("Finished generating load on NN, sending results to the reducer");
    } catch (Exception e) {
      System.err.println("Load generation failed: " + e.getMessage());
    } finally {
      progressThread.stopRunning();
      try { progressThread.join(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
    output.collect(new Text("Status"), new IntWritable(0));
  }
}
