package org.apache.calcite.jdbc;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.basic.BasicType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class SqrlToCalciteTableTranslator {
  Optional<NamePath> context;
  Namespace namespace;

  //TODO: May need to persist table
  public Table get(String table) {
    NamePath path = NamePath.parse(table);
    Optional<ai.dataeng.sqml.planner.Table> tableOptional = namespace.lookup(path);
    if (tableOptional.isEmpty()) {
      throw new RuntimeException(String.format("Could not find table %s", table));
    }

    return new CalciteTable(tableOptional.get(), table,
        toCalciteFields(tableOptional.get().getFields()));
  }

  private List<RelDataTypeField> toCalciteFields(ShadowingContainer<Field> fields) {
    List<RelDataTypeField> calciteFields = new ArrayList<>();
    int i = 0;
    for (Field field : fields) {
      Optional<RelDataTypeField> calciteField = toCalciteField(field, i);
      if (calciteField.isPresent()) {
        calciteFields.add(calciteField.get());
        i++;
      }
    }

    return calciteFields;
  }

  private Optional<RelDataTypeField> toCalciteField(Field field, int index) {
    if (field instanceof Relationship) {
      return Optional.empty();
    } else if (!(field instanceof Column)) {
      throw new RuntimeException(String.format("Unknown column type", field.getClass().getName()));
    }
    Column column = (Column) field;

    CalciteField calciteField = new CalciteField(field.getName(), index,
        toDataType(column.getType()), column);

    return Optional.of(calciteField);
  }

  private RelDataType toDataType(BasicType column) {
   return new BasicSqlType(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM,
        toSqlTypeName(column));
  }

  private SqlTypeName toSqlTypeName(BasicType column) {
    switch (column.getName()) {
      case "INTEGER":
        return SqlTypeName.INTEGER;
      case "BOOLEAN":
        return SqlTypeName.BOOLEAN;
      case "STRING":
        return SqlTypeName.VARCHAR;
      //todo: remaining
    }
    throw new RuntimeException(String.format(
        "Unrecognized type ", column.getClass().getName()));
  }
}
