package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.schema.Field;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class AbstractSqrlTable extends AbstractTable implements QueryableTable {

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }


  //This is only here so calcite can set up the correct model
  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    throw new RuntimeException("");
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  public List<String> getPrimaryKeys() {
    return List.of("_uuid");
  }

  public RelDataTypeField getField(Field field) {
    for (RelDataTypeField field1 : this.getRowType(null).getFieldList()) {
      if (field1.getName().equalsIgnoreCase(field.getName().getCanonical())) {
        return field1;
      }
    }
    throw new RuntimeException("Could not find sqrl field in calcite table");
  }

  public RelDataTypeField getTimestamp() {
    for (RelDataTypeField field1 : this.getRowType(null).getFieldList()) {
      if (field1.getName().equalsIgnoreCase("_ingest_time")) {
        return field1;
      }
    }
    throw new RuntimeException("Could not find sqrl timestamp in calcite table");
  }

  public abstract void addField(Field field, RelDataTypeField relDataTypeField);
}
