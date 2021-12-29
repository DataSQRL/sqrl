package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

public class GrowableRecordType extends RelDataTypeImpl {
  private List<RelDataTypeField> fields;
  private final DatasetOrTable table;

  public GrowableRecordType(List<RelDataTypeField> regFields, DatasetOrTable table) {
    this.fields = regFields;
    this.table = table;
    computeDigest();
  }

  @Override public List<RelDataTypeField> getFieldList() {
    return fields;
  }

  @Override public int getFieldCount() {
    return fields.size();
  }

  /**
   * Creates field on get
   */
  @Override public RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {
    //This comes in as a select star (*), not sure why
    if (fieldName.isEmpty()) {
      return new SqrlCalciteField(fieldName, this.fields.size(),
          new BasicSqlType(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM, SqlTypeName.INTEGER), null);
    }

    //Look for field in cache, duplicate fields cause ambiguous column errors
    for (RelDataTypeField f : fields) {
      if (f.getName().equalsIgnoreCase(fieldName)) {
        return f;
      }
    }

    Table table = (Table)this.table;
    NamePath namePath = NamePath.parse(fieldName);

    if (namePath.getPrefix().isPresent()) {
      table = table.walk(namePath.getPrefix().get())
          .orElseThrow(()->
              new RuntimeException(String.format("Could not find table %s", namePath.getPrefix().get())));
    }

    Field field = table.getField(namePath.getLast());
    if (field == null) {
      throw new RuntimeException(String.format("Could not find field %s", namePath.getLast()));
    }

    RelDataTypeField calciteField = toCalciteField(fieldName, field);
    this.fields.add(calciteField);

    // If a new field is added, we should re-compute the digest.
    computeDigest();
    return calciteField;
  }

  private RelDataTypeField toCalciteField(String fieldName, Field field) {
      return SqrlToCalciteTableTranslator.toCalciteField(fieldName, field, this.fields.size())
          .orElseThrow(
              ()->
                  new RuntimeException("Could not convert field."));
  }

  @Override public List<String> getFieldNames() {
    return fields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
  }

  @Override public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.<SqlTypeName>emptyList());
  }

  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(DynamicRecordRow").append(getFieldNames()).append(")");
  }

  @Override public boolean isStruct() {
    return true;
  }

  @Override public RelDataTypeFamily getFamily() {
    return getSqlTypeName().getFamily();
  }


  public StructKind getStructKind() {
    return StructKind.PEEK_FIELDS;
  }
}