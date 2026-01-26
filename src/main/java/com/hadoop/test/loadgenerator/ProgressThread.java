package com.hadoop.test.loadgenerator;

import org.apache.hadoop.mapred.Reporter;

public class ProgressThread extends Thread {
  private volatile boolean keepGoing = true;
  private final Reporter reporter;

  public ProgressThread(Reporter r) {
    this.reporter = r;
  }

  @Override
  public void run() {
    while (keepGoing) {
      try {
        Thread.sleep(30 * 1000);
      } catch (InterruptedException e) {
        // ignore
      }
      reporter.progress();
    }
  }

  public void stopRunning() {
    this.keepGoing = false;
  }
}
