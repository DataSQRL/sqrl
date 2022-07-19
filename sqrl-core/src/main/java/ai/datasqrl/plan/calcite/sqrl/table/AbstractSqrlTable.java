package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Field;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
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

@EqualsAndHashCode
public abstract class AbstractSqrlTable extends AbstractTable implements QueryableTable {

  protected final String nameId;

  protected AbstractSqrlTable(@NonNull Name nameId) {
    this.nameId = nameId.getCanonical();
  }

  public String getNameId() {
    return nameId;
  }

  public RelDataTypeField getField(Name nameId) {
    return getRowType().getField(nameId.getCanonical(),true,false);
  }

  public abstract RelDataType getRowType();

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return getRowType();
  }

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
  public java.lang.reflect.Type getElementType() {
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

  public RelDataTypeField getTimestampColumn() {
    for (RelDataTypeField field1 : this.getRowType(null).getFieldList()) {
      if (field1.getName().equalsIgnoreCase("_ingest_time")) {
        return field1;
      }
    }
    throw new RuntimeException("Could not find sqrl timestamp in calcite table");
  }

  public abstract void addField(RelDataTypeField relDataTypeField);
}
