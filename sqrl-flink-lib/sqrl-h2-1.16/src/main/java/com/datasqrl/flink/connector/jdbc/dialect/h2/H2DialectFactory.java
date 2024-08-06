/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink.connector.jdbc.dialect.h2;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

public class H2DialectFactory implements JdbcDialectFactory {
  @Override
  public boolean acceptsURL(String url) {
    return url.startsWith("jdbc:h2:");
  }

  @Override
  public JdbcDialect create() {
    return new H2Dialect();
  }
}
