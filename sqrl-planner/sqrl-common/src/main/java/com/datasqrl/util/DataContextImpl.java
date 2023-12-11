package com.datasqrl.util;

import com.datasqrl.calcite.SqrlFramework;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.QueryProviderImpl;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;

import java.util.HashMap;
import java.util.Optional;
import org.apache.calcite.schema.Schemas;


public class DataContextImpl implements DataContext {

  private final Supplier<List<Object[]>> dataSuppler;
  SqrlFramework framework;

  public DataContextImpl(SqrlFramework framework, Supplier<List<Object[]>> dataSuppler) {
    this.framework = framework;
    this.dataSuppler = dataSuppler;
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
      @SneakyThrows
      public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
        return (Enumerator<T>) Linq4j.asEnumerable(dataSuppler.get())
                    .enumerator();
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