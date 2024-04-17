/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.config.JdbcDialect;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Loads via ServiceLoader
 */
public class JdbcDDLServiceLoader {

  public Optional<JdbcDDLFactory> load(JdbcDialect dialect) {
    ServiceLoader<JdbcDDLFactory> factories = ServiceLoader.load(JdbcDDLFactory.class);
    for (JdbcDDLFactory factory : factories) {
      if (factory.getDialect().equals(dialect)) {
        return Optional.of(factory);
      }
    }

    return Optional.empty();
  }

}
