package com.hortonworks.spark.sql.hive.llap.util;

import com.google.common.base.Preconditions;
import com.hortonworks.hwc.plan.HwcPlannerStatistics;
import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.common.Column;
import com.hortonworks.spark.sql.hive.llap.common.DescribeTableOutput;


import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.SubmitWorkInfo;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;

public class JobUtil {

  public static final String SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id";
  private static Logger LOG = LoggerFactory.getLogger(JobUtil.class);
  public static final String LLAP_HANDLE_ID = "handleid";
  public static final String SESSION_QUERIES_FOR_GET_NUM_SPLITS = "llap.session.queries.for.get.num.splits";

  public static JobConf createJobConf(Map<String, String> options, String queryString) {
    JobConf jobConf = new JobConf(SparkContext.getOrCreate().hadoopConfiguration());
    jobConf.set("hive.llap.zk.registry.user", "hive");
    jobConf.set("llap.if.hs2.connection", HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options));
    if (queryString != null) {
      jobConf.set("llap.if.query", queryString);
    }
    jobConf.set("llap.if.user", HWConf.USER.getFromOptionsMap(options));
    jobConf.set("llap.if.pwd", HWConf.PASSWORD.getFromOptionsMap(options));
    if (options.containsKey("default.db")) {
      jobConf.set("llap.if.database", HWConf.DEFAULT_DB.getFromOptionsMap(options));
    }
    // Always create a new Hive connection for a data frame or rdd operation. This would help if any of the operation
    // re-uses the Data source reader factory that might be referring to Hive session specific scratch directory.
    // Need to pass new handleId for each operation to ensure allocation of new session and also make sure none of
    // the operation closes a connection opened by another rdd/df operation.
    String handleId = UUID.randomUUID().toString();
    options.put("handleid", handleId);
    jobConf.set("llap.if.handleid", options.get(LLAP_HANDLE_ID));
    LOG.info("Assigned handle ID:" + handleId + " for the current operation.");

    if (options.containsKey(SESSION_QUERIES_FOR_GET_NUM_SPLITS)) {
      jobConf.set(SESSION_QUERIES_FOR_GET_NUM_SPLITS, options.get(SESSION_QUERIES_FOR_GET_NUM_SPLITS));
    }
    jobConf.set("llap.if.use.new.split.format", "true");

    return jobConf;
  }


  public static byte[] serializeJobConf(JobConf jobConf) throws IOException {
    // on local, jobConf serialized byte[] was around around 84kb
    // let's keep it 200_000 to prevent multiple array copies.
    ByteArrayOutputStream confByteArrayStream = new ByteArrayOutputStream(200_000);

    try (DataOutputStream confByteData = new DataOutputStream(confByteArrayStream)) {
      jobConf.write(confByteData);
    }
    return confByteArrayStream.toByteArray();
  }

  public static String getSqlExecutionIdAtDriver() {
    return SparkSession.getActiveSession().get().sparkContext().getLocalProperty(SPARK_SQL_EXECUTION_ID);
  }

  public static String getSqlExecutionIdAtExecutor() {
    return TaskContext.get().getLocalProperty(SPARK_SQL_EXECUTION_ID);
  }

  public static void replaceSparkHiveDriver() throws Exception {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while(drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      String driverName = driver.getClass().getName();
      LOG.debug("Found a registered JDBC driver {}", driverName);
      if(driverName.endsWith("HiveDriver")) {
        LOG.debug("Deregistering {}", driverName);
        DriverManager.deregisterDriver(driver);
      } else {
        LOG.debug("Not deregistering the {}", driverName);
      }
    }
    DriverManager.registerDriver((Driver) Class.forName("shadehive.org.apache.hive.jdbc.HiveDriver").newInstance());
  }

  public static TaskAttemptID getTaskAttemptID(LlapInputSplit split) throws IOException {
    //Get pseudo-ApplicationId to submit task attempt from external client
    SubmitWorkInfo submitWorkInfo = SubmitWorkInfo.fromBytes(split.getPlanBytes());
    ApplicationId appId = submitWorkInfo.getFakeAppId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    //Create TaskAttemptID from Spark TaskContext (TaskType doesn't matter)
    return new TaskAttemptID(new TaskID(jobId, TaskType.MAP, TaskContext.get().partitionId()), TaskContext.get().attemptNumber());
  }

  public static RecordReader<?, ArrowWrapperWritable> getLlapArrowBatchRecordReader(
          LlapInputSplit split, JobConf conf, long arrowAllocatorMax, String attemptId) throws IOException {
    //Use per-task allocator for accounting only, no need to reserve per-task memory
    long childAllocatorReservation = 0L;
    //Break out accounting of direct memory per-task, so we can check no memory is leaked when task is completed
    BufferAllocator allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(arrowAllocatorMax).newChildAllocator(
            attemptId,
            childAllocatorReservation,
            arrowAllocatorMax);
    LlapBaseInputFormat input = new LlapBaseInputFormat(true, allocator);
    return input.getRecordReader(split, conf, null);
  }

  /**
   *
   * Makes a jdbc call and gets the output of `desc formatted {table}`. Based on that populates
   *  - sizeInBytes
   *  - numRows
   *  in HwcPlannerStatistics.
   *  Currently spark supports these two only for custom datasources.
   *
   *  Different datasources may use this method to get the table statistics if they are implementing {@link org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics}
   *
   * @param database database name
   * @param table table name
   * @param conn jdbc connection instance
   * @return HwcPlannerStatistics, throws exception if numRows or totalSize is not found.
   */
  public static HwcPlannerStatistics getStatisticsForTable(String database, String table, Connection conn) {
    DescribeTableOutput describeTable = DefaultJDBCWrapper.describeTable(conn, database, table);

    String totalSize = null;
    String numRows = null;

    for (Column column : describeTable.getDetailedTableInfoColumns()) {

      if (column.getDataType() != null) {
        if (column.getDataType().trim().equals("numRows")) {
          numRows = column.getComment();
        } else if (column.getDataType().trim().equals("totalSize")) {
          totalSize = column.getComment();
        }
      }

      if (totalSize != null && numRows != null) {
        break;
      }
    }

    Preconditions.checkNotNull(numRows, "numRows found null for table - " + table);
    Preconditions.checkNotNull(totalSize, "totalSize found null for table - " + table);

    return new HwcPlannerStatistics(OptionalLong.of(Long.parseLong(totalSize)),
            OptionalLong.of(Long.parseLong(numRows)));
  }

}
