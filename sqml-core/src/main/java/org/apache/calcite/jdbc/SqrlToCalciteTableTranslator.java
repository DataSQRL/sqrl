package org.apache.calcite.jdbc;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.basic.BasicType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.CalciteTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.CalciteField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Accepts a sqrl schema and translates it to a calcite schema.
 */
public class SqrlToCalciteTableTranslator {
  Optional<NamePath> context;
  Namespace namespace;

  public SqrlToCalciteTableTranslator(Optional<NamePath> context,
      Namespace namespace) {
    this.context = context;
    this.namespace = namespace;
  }

  Map<String, Table> tableMap = new HashMap<>();
  //TODO: May need to persist table
  public Table get(String table) {
    if (tableMap.containsKey(table)) {
      return tableMap.get(table);
    }

    NamePath path;

    if (table.startsWith("@")) {
      if (context.isEmpty()) throw new RuntimeException(String.format("Could not find context for table: ", table));
      path = context.get();
    } else {
      path = NamePath.parse(table);
    }

    Optional<ai.dataeng.sqml.planner.Table> tableOptional = namespace.lookup(path);
    if (tableOptional.isEmpty()) {
      throw new RuntimeException(String.format("Could not find table %s", table));
    }

    CalciteTable calciteTable = new CalciteTable(tableOptional.get(), table,
        toCalciteFields(tableOptional.get().getFields().visibleIterator()));

    tableMap.put(table, calciteTable);
    return calciteTable;
  }

  private List<RelDataTypeField> toCalciteFields(Iterator<Field> fields) {
    List<RelDataTypeField> calciteFields = new ArrayList<>();
    int i = 0;
    for (Iterator<Field> it = fields; it.hasNext(); ) {
      Field field = it.next();
      System.out.println(field.getName());
      Optional<RelDataTypeField> calciteField = toCalciteField(field.getName().toString(), field, i);
      if (calciteField.isPresent()) {
        calciteFields.add(calciteField.get());
        i++;
      }
    }

    return calciteFields;
  }

  public static Optional<RelDataTypeField> toCalciteField(String fieldName, Field field,
      int index) {
    if (field instanceof Relationship) {
      //Relationship types can happen, for example `expr.col := count(rel)`
      // Just return an integer type
      return Optional.of(new CalciteField(fieldName, index,
          new BasicSqlType(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM, SqlTypeName.INTEGER), null));
    } else if (!(field instanceof Column)) {
      throw new RuntimeException(String.format("Unknown column type", field.getClass().getName()));
    }
    Column column = (Column) field;

    CalciteField calciteField = new CalciteField(fieldName, index,
        toDataType(column.getType()), column);

    return Optional.of(calciteField);
  }

  private static RelDataType toDataType(BasicType column) {
   return new BasicSqlType(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM,
        toSqlTypeName(column));
  }

  private static SqlTypeName toSqlTypeName(BasicType column) {
    switch (column.getName()) {
      case "INTEGER":
        return SqlTypeName.INTEGER;
      case "BOOLEAN":
        return SqlTypeName.BOOLEAN;
      case "STRING":
        return SqlTypeName.VARCHAR;
      case "UUID":
        return SqlTypeName.VARCHAR;
      case "FLOAT":
        return SqlTypeName.FLOAT;
      //todo: remaining
    }
    throw new RuntimeException(String.format(
        "Unrecognized type %s", column.getClass().getName()));
  }
}
