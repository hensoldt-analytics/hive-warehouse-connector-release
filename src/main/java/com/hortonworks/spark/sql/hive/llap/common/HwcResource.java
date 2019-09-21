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

package com.hortonworks.spark.sql.hive.llap.common;

import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * HwcResource - Holds all the resources/ids that have been used during read/write execution cycle.
 * As of now, HwcResource instance is always associated to sessionId of HiveWarehouseSession.
 */
public class HwcResource implements Closeable {

  private static Logger LOG = LoggerFactory.getLogger(HwcResource.class);

  private final String llapHandleId;
  private final CommonBroadcastInfo commonBroadcastInfo;

  public HwcResource(String llapHandleId, CommonBroadcastInfo commonBroadcastInfo) {
    this.llapHandleId = llapHandleId;
    this.commonBroadcastInfo = commonBroadcastInfo;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing reader llap resource: {}", llapHandleId);
    LlapBaseInputFormat.close(llapHandleId);

    LOG.info("Closing reader broadcast variables: {}", commonBroadcastInfo);
    if (commonBroadcastInfo != null) {
      if (commonBroadcastInfo.getPlanSplit().isValid()) {
        commonBroadcastInfo.getPlanSplit().destroy();
      }
      if (commonBroadcastInfo.getSchemaSplit().isValid()) {
        commonBroadcastInfo.getSchemaSplit().destroy();
      }
    }
  }
}
