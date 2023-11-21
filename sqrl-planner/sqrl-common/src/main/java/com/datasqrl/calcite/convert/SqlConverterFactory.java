package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.util.ServiceLoaderDiscovery;

public class SqlConverterFactory {

  public static RelToSqlNode get(Dialect dialect) {
    return ServiceLoaderDiscovery.get(RelToSqlNode.class, e->e.getDialect().name(),
        dialect.name());
  }
}
