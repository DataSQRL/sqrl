/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.memory;

import com.datasqrl.plan.memory.rel.InMemoryEnumerableTableScan;
import lombok.AllArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcQueryProvider;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

/**
 * The LocalDataContext is responsible for fetching, storing, and managing the data that is used by
 * the physical plans. It is used during physical plan evaluation to fetch data.
 */
@AllArgsConstructor
public class LocalDataContext implements DataContext {

  SchemaPlus schemaPlus;

  /**
   * Entry point for code generated procedures
   *
   * @see InMemoryEnumerableTableScan#implement(EnumerableRelImplementor, Prefer)
   */
  @Override
  public SchemaPlus getRootSchema() {
    return schemaPlus;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return null;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return JdbcQueryProvider.INSTANCE;
  }

  @Override
  public Object get(String s) {
    return null;
  }
}
