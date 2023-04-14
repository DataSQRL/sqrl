/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.config.SinkFactory.SinkFactoryContext;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.tables.TableConfig;
import lombok.Value;

public interface SinkFactory<ENGINE_SINK, C extends SinkFactoryContext> extends BaseConnectorFactory {
  String getSinkType();

  ENGINE_SINK create(C context);

  interface SinkFactoryContext {

  }

  //todo move to flink sink dir
  @Value
  class FlinkSinkFactoryContext implements SinkFactoryContext {
    String tableName;
    DataSystemConnectorConfig config;
    TableConfig tableConfig;
    ErrorCollector errors;
  }
}
