/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import java.lang.reflect.Type;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.apache.calcite.linq4j.Enumerator;
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

/**
 * A relational table is a Calcite table that represents a relation in standard relational algebra.
 * <p>
 * This is the base class for all relational tables that the transpiler creates to represent the
 * logical SQRL tables that users import or define in their scripts.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public abstract class AbstractRelationalTable extends AbstractTable implements
    Comparable<AbstractRelationalTable>, QueryableTable {

  @EqualsAndHashCode.Include
  protected final String nameId;

  protected AbstractRelationalTable(@NonNull Name nameId) {
    this.nameId = nameId.getCanonical();
  }

  public String getNameId() {
    return nameId;
  }

  public RelDataTypeField getField(Name nameId) {
    return getRowType().getField(nameId.getCanonical(), true, false);
  }

  public abstract RelDataType getRowType();

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return getRowType();
  }

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }

  @Override
  public int compareTo(AbstractRelationalTable other) {
    return this.getNameId().compareTo(other.getNameId());
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return (Queryable<T>) new AbstractTableQueryable<Object[]>(queryProvider, schema, this, tableName) {
      @Override
      public Enumerator<Object[]> enumerator() {
        return null;
//        return Linq4j.asEnumerable(List.of(new Object[]{}))
//            .enumerator();
      }
    };
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }
}
