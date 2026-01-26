package com.hadoop.test.loadgenerator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;

public class EmptySplit implements InputSplit {
  @Override
  public void write(DataOutput out) throws IOException {}
  @Override
  public void readFields(DataInput in) throws IOException {}
  @Override
  public long getLength() { return 0L; }
  @Override
  public String[] getLocations() { return new String[0]; }
}
