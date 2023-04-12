/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import java.util.Optional;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.TableDescriptor;

public class JdbcSinkFactory
    implements TableDescriptorSinkFactory {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkType() {
    return JdbcDataSystemConnectorConfig.SYSTEM_TYPE;
  }

  @Override
  public TableDescriptor.Builder create(FlinkSinkFactoryContext context) {
      JdbcDataSystemConnectorConfig config = (JdbcDataSystemConnectorConfig)context.getConfig();

      TableDescriptor.Builder builder = TableDescriptor.forConnector("jdbc")
          .option(JdbcConnectorOptions.URL, config.getDbURL())
          .option("table-name", context.getTableName());
      Optional.ofNullable(config.getDriverName())
          .map(u->builder.option(JdbcConnectorOptions.DRIVER, config.getDriverName()));
      Optional.ofNullable(config.getUser())
          .map(u->builder.option(JdbcConnectorOptions.USERNAME, u));
      Optional.ofNullable(config.getPassword())
          .map(p->builder.option(JdbcConnectorOptions.PASSWORD, p));

      return builder;
  }
}
