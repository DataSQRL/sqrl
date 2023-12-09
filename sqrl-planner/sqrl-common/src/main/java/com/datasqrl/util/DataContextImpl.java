package com.datasqrl.util;

import com.datasqrl.calcite.SqrlFramework;
import java.lang.reflect.Type;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.QueryProviderImpl;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;

import java.util.HashMap;
import java.util.Optional;
import org.apache.calcite.schema.Schemas;


public class DataContextImpl implements DataContext {
  SqrlFramework framework;

  public DataContextImpl(SqrlFramework framework) {
    this.framework = framework;
  }

  private HashMap<String, Object> carryover;

  @Override
  public SchemaPlus getRootSchema() {
    return framework.getSchema().plus();
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return framework.getTypeFactory();
  }

  @Override
  public QueryProvider getQueryProvider() {
    return new QueryProviderImpl() {
      @Override
      public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
        return null;
      }
    };
  }

  @Override
  public synchronized Object get(String name) {
    return Optional.ofNullable(carryover.get(name))
        .orElse(name);
  }


  public void setCarryover(HashMap<String, Object> carryover) {
    this.carryover = carryover;
  }
}