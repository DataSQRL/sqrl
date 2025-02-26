/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.SqlDDLStatement;

public interface JdbcDDLFactory {
  JdbcDialect getDialect();

  SqlDDLStatement createTable(EngineSink table);

  SqlDDLStatement createIndex(IndexDefinition indexDefinitions);
}
