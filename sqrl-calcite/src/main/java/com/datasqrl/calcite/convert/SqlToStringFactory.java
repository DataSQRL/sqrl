package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.util.ServiceLoaderDiscovery;

public class SqlToStringFactory {

  public static SqlNodeToString get(Dialect dialect) {
    return ServiceLoaderDiscovery.get(
        SqlNodeToString.class, e -> e.getDialect().name(), dialect.name());
  }
}
