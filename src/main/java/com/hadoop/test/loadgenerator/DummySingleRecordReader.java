package com.hadoop.test.loadgenerator;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

public class DummySingleRecordReader implements RecordReader<LongWritable, Text> {
  private boolean sentOneRecord = false;

  @Override
  public boolean next(LongWritable key, Text value) throws IOException {
    if (!sentOneRecord) {
      key.set(1);
      value.set("dummy");
      sentOneRecord = true;
      return true;
    }
    return false;
  }

  @Override
  public LongWritable createKey() { return new LongWritable(); }
  @Override
  public Text createValue() { return new Text(); }
  @Override
  public long getPos() throws IOException { return sentOneRecord ? 1 : 0; }
  @Override
  public void close() throws IOException {}
  @Override
  public float getProgress() throws IOException { return sentOneRecord ? 1f : 0f; }
}
