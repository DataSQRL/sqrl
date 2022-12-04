/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
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
