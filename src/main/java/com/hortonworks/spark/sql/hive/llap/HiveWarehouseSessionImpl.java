/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.sql.hive.llap.common.HwcResource;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.TriFunction;
import org.apache.commons.lang.BooleanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hortonworks.spark.sql.hive.llap.HWConf.*;

public class HiveWarehouseSessionImpl implements com.hortonworks.hwc.HiveWarehouseSession {
  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseSessionImpl.class);

  public static final String SESSION_CLOSED_MSG = "Session is closed!!! No more operations can be performed. Please create a new HiveWarehouseSession";
  public static final String HWC_SESSION_ID_KEY = "hwc_session_id";

  static String HIVE_WAREHOUSE_CONNECTOR_INTERNAL = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR;

  protected HiveWarehouseSessionState sessionState;

  protected Supplier<Connection> getConnector;

  protected TriFunction<Connection, String, String, DriverResultSet> executeStmt;

  protected TriFunction<Connection, String, String, Boolean> executeUpdate;

  /**
   * Keeps resources handles by session id. Resources are of types @{@link HwcResource}
   * {@link #close()} method closes all of them and session as well.
   */
  private static final Map<String, Set<HwcResource>> RESOURCE_IDS_BY_SESSION_ID = new HashMap<>();

  private final String sessionId;
  private final AtomicReference<HwcSessionState> hwcSessionStateRef;


  public HiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
    this.sessionState = sessionState;
    this.sessionId = UUID.randomUUID().toString();
    getConnector = () -> DefaultJDBCWrapper.getConnector(sessionState);
    executeStmt = (conn, database, sql) ->
        DefaultJDBCWrapper.executeStmt(conn, database, sql, MAX_EXEC_RESULTS.getInt(sessionState));
    executeUpdate = (conn, database, sql) ->
      DefaultJDBCWrapper.executeUpdate(conn, database, sql);
    sessionState.session.listenerManager().register(new LlapQueryExecutionListener());
    sessionState.session.sparkContext().addSparkListener(new HwcSparkListener());
    hwcSessionStateRef = new AtomicReference<>(HwcSessionState.OPEN);
    LOG.info("Created a new HWC session: {}", sessionId);
  }

  private enum HwcSessionState {
    OPEN, CLOSED;
  }

  private void ensureSessionOpen() {
    Preconditions.checkState(HwcSessionState.OPEN.equals(hwcSessionStateRef.get()), SESSION_CLOSED_MSG);
  }

  public Dataset<Row> q(String sql) {
    ensureSessionOpen();
    return executeQuery(sql);
  }

  public Dataset<Row> executeQuery(String sql) {
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("query", sql)
        .option(HWC_SESSION_ID_KEY, sessionId);;
    return dfr.load();
  }

  static void addResourceIdToSession(String sessionId, HwcResource resourceId) {
    LOG.info("Adding resource: {} to current session: {}", resourceId, sessionId);
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      RESOURCE_IDS_BY_SESSION_ID.putIfAbsent(sessionId, new HashSet<>());
      RESOURCE_IDS_BY_SESSION_ID.get(sessionId).add(resourceId);
    }
  }

  static void closeAndRemoveResourceFromSession(String sessionId, HwcResource hwcResource) throws IOException {
    Set<HwcResource> hwcResources;
    boolean resourcePresent;
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      hwcResources = RESOURCE_IDS_BY_SESSION_ID.get(sessionId);
      resourcePresent = hwcResources != null && hwcResources.remove(hwcResource);
    }
    if (resourcePresent) {
      LOG.info("Remove and close resource: {} from current session: {}", hwcResource, sessionId);
      hwcResource.close();
    }
  }

  private static void closeSessionResources(String sessionId) throws IOException {
    LOG.info("Closing all resources for current session: {}", sessionId);
    Set<HwcResource> hwcResources;
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      hwcResources = RESOURCE_IDS_BY_SESSION_ID.remove(sessionId);
    }
    if (hwcResources != null && !hwcResources.isEmpty()) {
      for (HwcResource resource : hwcResources) {
        resource.close();
      }
    }
  }


  public Dataset<Row> execute(String sql) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
      return drs.asDataFrame(session());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean executeUpdate(String sql) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      return executeUpdate.apply(conn, DEFAULT_DB.getString(sessionState), sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean executeUpdateInternal(String sql, Connection conn) {
    ensureSessionOpen();
    return executeUpdate.apply(conn, DEFAULT_DB.getString(sessionState), sql);
  }

  public Dataset<Row> executeInternal(String sql, Connection conn) {
    DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
    return drs.asDataFrame(session());
  }

  public Dataset<Row> table(String sql) {
    ensureSessionOpen();
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("table", sql)
        .option(HWC_SESSION_ID_KEY, sessionId);
    return dfr.load();
  }

  public SparkSession session() {
    ensureSessionOpen();
    return sessionState.session;
  }

  // Exposed for Python side.
  public HiveWarehouseSessionState sessionState() {
    return sessionState;
  }

  SparkConf conf() {
    return sessionState.session.sparkContext().getConf();
  }

  /* Catalog helpers */
  public void setDatabase(String name) {
    ensureSessionOpen();
    HWConf.DEFAULT_DB.setString(sessionState, name);
  }

  public Dataset<Row> showDatabases() {
    ensureSessionOpen();
    return execute(HiveQlUtil.showDatabases());
  }

  public Dataset<Row> showTables() {
    ensureSessionOpen();
    return execute(HiveQlUtil.showTables(DEFAULT_DB.getString(sessionState)));
  }

  public Dataset<Row> describeTable(String table) {
    ensureSessionOpen();
    return execute(HiveQlUtil.describeTable(DEFAULT_DB.getString(sessionState), table));
  }

  public void dropDatabase(String database, boolean ifExists, boolean cascade) {
    executeUpdate(HiveQlUtil.dropDatabase(database, ifExists, cascade));
  }

  public void dropTable(String table, boolean ifExists, boolean purge) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      executeUpdateInternal(HiveQlUtil.useDatabase(DEFAULT_DB.getString(sessionState)), conn);
      String dropTable = HiveQlUtil.dropTable(table, ifExists, purge);
      executeUpdateInternal(dropTable, conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void createDatabase(String database, boolean ifNotExists) {
    executeUpdate(HiveQlUtil.createDatabase(database, ifNotExists));
  }

  public CreateTableBuilder createTable(String tableName) {
    ensureSessionOpen();
    return new CreateTableBuilder(this, DEFAULT_DB.getString(sessionState), tableName);
  }

  @Override
  public void close() {
    Preconditions.checkState(hwcSessionStateRef.compareAndSet(HwcSessionState.OPEN, HwcSessionState.CLOSED),
        SESSION_CLOSED_MSG);
    try {
      closeSessionResources(sessionId);
    } catch (IOException e) {
      throw new RuntimeException("Error while closing resources attached to session: " + sessionId);
    }
  }

}

