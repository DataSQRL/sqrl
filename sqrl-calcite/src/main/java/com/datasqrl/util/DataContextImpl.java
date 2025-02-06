package com.datasqrl.util;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.QueryProviderImpl;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.SchemaPlus;

import com.datasqrl.calcite.SqrlFramework;

import lombok.SneakyThrows;


public class DataContextImpl implements DataContext {

  private final Supplier<List<Object[]>> dataSuppler;
  SqrlFramework framework;

  public DataContextImpl(SqrlFramework framework, Supplier<List<Object[]>> dataSuppler) {
    this.framework = framework;
    this.dataSuppler = dataSuppler;
  }

  private Map<String, Object> contextVariables;

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
      @SneakyThrows
      public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
        return (Enumerator<T>) Linq4j.asEnumerable(dataSuppler.get())
                    .enumerator();
      }
    };
  }

  @Override
  public synchronized Object get(String name) {
    return Optional.ofNullable(contextVariables.get(name))
        .orElse(name);
  }


  /**
   *
   * Supported variables: {@link org.apache.calcite.DataContext.Variable}
   */
  public void setContextVariables(Map<String, Object> contextVariables) {
    this.contextVariables = contextVariables;
  }
}