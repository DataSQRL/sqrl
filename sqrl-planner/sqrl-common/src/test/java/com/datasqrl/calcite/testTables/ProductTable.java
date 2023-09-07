package com.datasqrl.calcite.testTables;

import lombok.Value;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProductTable extends AbstractQueryableTable implements
    CustomColumnResolvingTable {

  private final List<ProductTable.Student> students;

  List<String> names = List.of("_uuid", "productid", "name");

  public ProductTable() {
    this(Arrays.asList(new ProductTable.Student("123", 101, "Music"),
        new ProductTable.Student("123", 102, "Math"),
        new ProductTable.Student("123", 103, "Geometry"),
        new ProductTable.Student("123", 104, "Gym")));
  }

  public ProductTable(List<Student> students) {
    super(Object[].class);
    this.students = students;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 200);
    RelDataType arrayType = typeFactory.createArrayType(varcharType, -1);

    RelDataType rowType = typeFactory.createStructType(
        Arrays.asList(typeFactory.createSqlType(SqlTypeName.VARCHAR, 20),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            typeFactory.createSqlType(SqlTypeName.VARCHAR, 20)),
        Arrays.asList("_uuid$0", "id$1", "name$2"));
    return rowType;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return (Queryable<T>) new AbstractTableQueryable<Object[]>(queryProvider, schema, this,
        tableName) {
      @Override
      public Enumerator<Object[]> enumerator() {
        return Linq4j.asEnumerable(students.stream().map(
                student -> new Object[]{student.get_uuid(), student.getProductid(), student.getName()})
            .collect(Collectors.toList())).enumerator();
      }
    };
  }

  public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
      RelDataTypeFactory relDataTypeFactory, List<String> name) {
    //Look for shadowed column first, then remaining
    if (name.size() != 1) {
      return List.of();
    }

    RelDataType t = this.getRowType(relDataTypeFactory);
    for (int i = 0; i < names.size(); i++) {
      if (names.get(i).equalsIgnoreCase(name.get(0)) || t.getFieldList().get(i).getName()
          .equalsIgnoreCase(name.get(0))) {
        return List.of(Pair.of(t.getFieldList().get(i), List.of()));
      }
    }

    return List.of();
  }

  @Value
  public static class Student {

    String _uuid;
    int productid;
    String name;
  }
}