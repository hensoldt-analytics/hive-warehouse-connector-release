package com.hortonworks.spark.sql.hive.llap;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.hortonworks.spark.sql.hive.llap.HWConf.DISABLE_PRUNING_AND_PUSHDOWNS;
import static com.hortonworks.spark.sql.hive.llap.HWConf.USE_SPARK23X_SPECIFIC_READER;

/*
 * Driver:
 *   UserCode -> HiveWarehouseConnector -> HiveWarehouseDataSourceReader -> HiveWarehouseDataReaderFactory
 * Task serializer:
 *   HiveWarehouseDataReaderFactory (Driver) -> bytes -> HiveWarehouseDataReaderFactory (Executor task)
 * Executor:
 *   HiveWarehouseDataReaderFactory -> HiveWarehouseDataReader
 */
public class HiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport, WriteSupport {

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

  @Override public DataSourceReader createReader(DataSourceOptions options) {
    try {
      return getDataSourceReader(getOptions(options));
    } catch (IOException e) {
      LOG.error("Error creating {}", getClass().getName());
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType schema,
      SaveMode mode, DataSourceOptions options) {
    Map<String, String> params = getOptions(options);
    String stagingDirPrefix = HWConf.LOAD_STAGING_DIR.getFromOptionsMap(params);
    Path path = new Path(stagingDirPrefix);
    Configuration conf = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
    return Optional.of(getDataSourceWriter(jobId, schema, path, params, conf, mode));
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

  private static Map<String, String> getOptions(DataSourceOptions options) {
    return options.asMap();
  }

  protected DataSourceReader getDataSourceReader(Map<String, String> params) throws IOException {

    boolean useSpark23xReader = BooleanUtils.toBoolean(USE_SPARK23X_SPECIFIC_READER.getFromOptionsMap(params));
    boolean disablePruningPushdown = BooleanUtils.toBoolean(DISABLE_PRUNING_AND_PUSHDOWNS.getFromOptionsMap(params));

    LOG.info("Found reader configuration - {}={}, {}={}", USE_SPARK23X_SPECIFIC_READER.getQualifiedKey(), useSpark23xReader,
        DISABLE_PRUNING_AND_PUSHDOWNS.getQualifiedKey(), disablePruningPushdown);

    Preconditions.checkState(!(useSpark23xReader && disablePruningPushdown), HWConf.INVALID_READER_CONFIG_ERR_MSG);

    if (useSpark23xReader) {
      LOG.info("Using reader HiveWarehouseDataSourceReaderForSpark23x with column pruning disabled");
      return new HiveWarehouseDataSourceReaderForSpark23x(params);
    } else if (disablePruningPushdown) {
      LOG.info("Using reader HiveWarehouseDataSourceReader with column pruning and filter pushdown disabled");
      return new HiveWarehouseDataSourceReader(params);
    } else {
      LOG.info("Using reader PrunedFilteredHiveWarehouseDataSourceReader");
      return new PrunedFilteredHiveWarehouseDataSourceReader(params);
    }
  }

  protected DataSourceWriter getDataSourceWriter(String jobId, StructType schema,
                                                 Path path, Map<String, String> options, Configuration conf, SaveMode mode) {
    return new HiveWarehouseDataSourceWriter(options, jobId, schema, path, conf, mode);
  }

}
