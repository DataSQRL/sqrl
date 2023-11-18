package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.util.ServiceLoaderDiscovery;

public class SqlToStringFactory {

  public static SqlToString get(Dialect dialect) {
    return ServiceLoaderDiscovery.get(SqlToString.class, e->e.getDialect().name(),
        dialect.name());
  }
}
