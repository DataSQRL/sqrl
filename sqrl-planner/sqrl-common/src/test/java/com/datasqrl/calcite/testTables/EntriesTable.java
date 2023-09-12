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

public class EntriesTable extends AbstractQueryableTable {
  private final List<Entries> students;

  public EntriesTable() {
    this(Arrays.asList(new Entries("5", 20, 100, new double[]{1,2,3}),
        new Entries("1", 22, 101, new double[]{1,2,3}),
        new Entries("1", 22, 102, new double[]{1,2,3}),
        new Entries("2", 22, 101, new double[]{1,2,3}),
        new Entries("3", 21, 102, new double[]{1,2,3}),
        new Entries("4", 22, 103, new double[]{1,2,3})
        ));
  }

  public EntriesTable(List<Entries> students) {
    super(Object[].class);
    this.students = students;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType subtype = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    RelDataType arrayType = typeFactory.createArrayType(subtype, -1);

    RelDataType rowType = typeFactory.createStructType(
        Arrays.asList(typeFactory.createSqlType(SqlTypeName.VARCHAR, 20),
        typeFactory.createSqlType(SqlTypeName.INTEGER),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            arrayType),
        Arrays.asList("_uuid", "discount", "productid", "embedding"));
    return rowType;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return (Queryable<T>) new AbstractTableQueryable<Object[]>(queryProvider, schema, this, tableName) {
      @Override
      public Enumerator<Object[]> enumerator() {
        return Linq4j.asEnumerable(students.stream().map(student -> new Object[]{student.get_uuid(), student.getDiscount(),
                student.getProductId(), student.getEmbedding()})
            .collect(Collectors.toList()))
            .enumerator();
      }
    };
  }

  @Value
  public static class Entries {
    String _uuid;
    int discount;
    int productId;
    double[] embedding;
  }
}