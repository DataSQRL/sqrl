/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.dialect;

import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;

public interface JdbcDDLFactory {
  String getDialect();
  SqlDDLStatement createTable(EngineSink table);

  SqlDDLStatement createIndex(IndexDefinition indexDefinitions);
}
