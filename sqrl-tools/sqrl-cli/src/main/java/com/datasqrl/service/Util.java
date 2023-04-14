package com.datasqrl.service;

import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorConfig;
import com.google.common.collect.Iterables;
import java.util.List;

public class Util {

  public static JdbcDataSystemConnectorConfig getJdbcEngine(List<EngineConfiguration> engines) {
    //Extract JDBC engine
    JDBCEngineConfiguration jdbcConfig = Iterables.getOnlyElement(
        Iterables.filter(engines, JDBCEngineConfiguration.class));
    return jdbcConfig.getConfig();
  }

}
