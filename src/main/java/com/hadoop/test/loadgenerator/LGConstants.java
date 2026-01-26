package com.hadoop.test.loadgenerator;

import org.apache.hadoop.io.Text;

public class LGConstants {
  public static final Text OPEN_EXECTIME = new Text("OpenExecutionTime");
  public static final Text NUMOPS_OPEN = new Text("NumOpsOpen");
  public static final Text LIST_EXECTIME = new Text("ListExecutionTime");
  public static final Text NUMOPS_LIST = new Text("NumOpsList");
  public static final Text DELETE_EXECTIME = new Text("DeletionExecutionTime");
  public static final Text NUMOPS_DELETE = new Text("NumOpsDelete");
  public static final Text CREATE_EXECTIME = new Text("CreateExecutionTime");
  public static final Text NUMOPS_CREATE = new Text("NumOpsCreate");
  public static final Text WRITE_CLOSE_EXECTIME = new Text("WriteCloseExecutionTime");
  public static final Text NUMOPS_WRITE_CLOSE = new Text("NumOpsWriteClose");
  public static final Text ELAPSED_TIME = new Text("ElapsedTime");
  public static final Text TOTALOPS = new Text("TotalOps");

  public static final String LG_ROOT = "LG.root";
  public static final String LG_SCRIPTFILE = "LG.scriptFile";
  public static final String LG_MAXDELAYBETWEENOPS = "LG.maxDelayBetweenOps";
  public static final String LG_NUMOFTHREADS = "LG.numOfThreads";
  public static final String LG_READPR = "LG.readPr";
  public static final String LG_WRITEPR = "LG.writePr";
  public static final String LG_SEED = "LG.r";
  public static final String LG_NUMMAPTASKS = "LG.numMapTasks";
  public static final String LG_ELAPSEDTIME = "LG.elapsedTime";
  public static final String LG_STARTTIME = "LG.startTime";
  public static final String LG_FLAGFILE = "LG.flagFile";
}
