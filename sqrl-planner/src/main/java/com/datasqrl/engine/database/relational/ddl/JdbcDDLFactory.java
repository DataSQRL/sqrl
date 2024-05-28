/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;

public interface JdbcDDLFactory {
  JdbcDialect getDialect();

  default Optional<SqlNode> createCatalog() {
    return Optional.empty();
  }

  SqlDDLStatement createTable(EngineSink table);

  SqlDDLStatement createIndex(IndexDefinition indexDefinitions);
}
