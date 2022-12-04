/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.memory.table;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import com.datasqrl.plan.calcite.util.CalciteUtil;
import lombok.AllArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

@AllArgsConstructor
public class DataTable extends AbstractTable implements QueryableTable {

  private final List<RelDataTypeField> header;
  private final Collection<Object[]> elements;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    CalciteUtil.RelDataTypeBuilder builder = CalciteUtil.getRelTypeBuilder(relDataTypeFactory);
    header
        .forEach(builder::add);
    return builder.build();
  }

  /**
   * Returns an enumerable over a given projection of the fields.
   */
  @SuppressWarnings("unused")
  public Enumerable<Object> project(DataContext context) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return toEnumerator(elements);
      }
    };
  }

  public Type getElementType() {
    return Object[].class;
  }

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new AbstractTableQueryable<>(queryProvider, schema, this,
        tableName) {
      @Override
      public Enumerator<T> enumerator() {
        return toEnumerator(elements);
      }
    };
  }

  private <T> Enumerator<T> toEnumerator(Collection<Object[]> elements) {
    return (Enumerator<T>) Linq4j.asEnumerable(elements).enumerator();
  }
}