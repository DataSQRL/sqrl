/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.engine.pipeline.EngineStage;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.plan.global.OptimizedDAG.EngineSink;
import java.util.Optional;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class JdbcSinkFactory
    implements SinkFactory<Builder> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkName() {
    return "jdbc";
  }

  @Override
  public TableDescriptor.Builder create(EngineSink engineSink) {
    ExecutionEngine engineStage = engineSink.getStage().getEngine();
    JDBCEngine jdbcEngine = (JDBCEngine) engineStage;
    JdbcDataSystemConnectorConfig config = jdbcEngine.getConfig()
        .getConfig();

    TableDescriptor.Builder builder = TableDescriptor.forConnector("jdbc")
        .option(JdbcConnectorOptions.URL, config.getDbURL());
    Optional.ofNullable(config.getDriverName())
        .map(u->builder.option(JdbcConnectorOptions.DRIVER, config.getDriverName()));
    Optional.ofNullable(config.getUser())
        .map(u->builder.option(JdbcConnectorOptions.USERNAME, u));
    Optional.ofNullable(config.getPassword())
        .map(p->builder.option(JdbcConnectorOptions.PASSWORD, p));

    return builder;

  }
}
