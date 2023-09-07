package com.datasqrl.calcite.testTables;

import lombok.Value;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OrdersTable extends AbstractQueryableTable {
  private final List<Student> students;

  public OrdersTable() {
    this(Arrays.asList(new OrdersTable.Student("1", 10),
        new OrdersTable.Student("2", 20),
        new OrdersTable.Student("3", 30),
        new OrdersTable.Student("4", 40),
        new OrdersTable.Student("5", 50)
        ));
  }

  public OrdersTable(List<Student> students) {
    super(Object[].class);
    this.students = students;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 200);
    RelDataType arrayType = typeFactory.createArrayType(varcharType, -1);

    RelDataType rowType = typeFactory.createStructType(
        Arrays.asList(typeFactory.createSqlType(SqlTypeName.VARCHAR, 20),
        typeFactory.createSqlType(SqlTypeName.INTEGER)), Arrays.asList("_uuid", "id"));
    return rowType;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return (Queryable<T>) new AbstractTableQueryable<Object[]>(queryProvider, schema, this, tableName) {
      @Override
      public Enumerator<Object[]> enumerator() {
        return Linq4j.asEnumerable(students.stream().map(student -> new Object[]{student.get_uuid(), student.getId()})
            .collect(Collectors.toList()))
            .enumerator();
      }
    };
  }

  @Value
  public static class Student {
    String _uuid;
    int id;
  }
}