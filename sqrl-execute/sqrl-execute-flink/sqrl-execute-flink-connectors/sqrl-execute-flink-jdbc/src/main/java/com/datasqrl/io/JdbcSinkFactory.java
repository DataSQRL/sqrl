/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorFactory;
import java.util.Optional;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.TableDescriptor;

public class JdbcSinkFactory
    implements TableDescriptorSinkFactory {

  @Override
  public String getSinkType() {
    return JdbcDataSystemConnectorFactory.SYSTEM_NAME;
  }

  @Override
  public TableDescriptor.Builder create(FlinkSinkFactoryContext context) {
    JdbcDataSystemConnector jdbc = new JdbcDataSystemConnectorFactory().initialize(context.getTableConfig()
        .getConnectorConfig());

    TableDescriptor.Builder builder = TableDescriptor.forConnector("jdbc")
        .option(JdbcConnectorOptions.URL, jdbc.getUrl())
        .option("table-name", context.getTableName());
    Optional.ofNullable(jdbc.getDriver())
        .map(u->builder.option(JdbcConnectorOptions.DRIVER, jdbc.getDriver()));
    Optional.ofNullable(jdbc.getUser())
        .map(u->builder.option(JdbcConnectorOptions.USERNAME, u));
    Optional.ofNullable(jdbc.getPassword())
        .map(p->builder.option(JdbcConnectorOptions.PASSWORD, p));

    return builder;
  }
}
