package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.util.ServiceLoaderDiscovery;

public class SqlConverterFactory {

  public static SqlConverter get(Dialect dialect) {
    return ServiceLoaderDiscovery.get(SqlConverter.class, e->e.getDialect().name(),
        dialect.name());
  }
}
