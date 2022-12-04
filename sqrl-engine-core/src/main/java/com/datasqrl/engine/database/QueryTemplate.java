package com.datasqrl.engine.database;

import com.datasqrl.config.provider.DatabaseConnectionProvider;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class QueryTemplate {

  RelNode relNode;
  DatabaseConnectionProvider dbConnection;
  //TODO: add parameters

}
