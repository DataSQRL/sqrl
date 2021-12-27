package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

public class GrowableRecordType extends RelDataTypeImpl {
  private RelDataTypeFactory typeFactory;
  private List<RelDataTypeField> fields = new ArrayList<>();
  private final DatasetOrTable table;

  /** Creates a DynamicRecordTypeImpl. */
  public GrowableRecordType(RelDataTypeFactory typeFactory,
      List<RelDataTypeField> regFields, DatasetOrTable table) {
    this.typeFactory = typeFactory;
    this.fields = regFields;
    this.table = table;
    computeDigest();
//    RelDataTypeFieldImpl f = new RelDataTypeFieldImpl("orders", 0, this.typeFactory.createSqlType(SqlTypeName.BOOLEAN));
//    fieldMap.put("x", f);
//    fields.add(f);
  }

  @Override public List<RelDataTypeField> getFieldList() {
    return fields;
  }

  @Override public int getFieldCount() {
    return fields.size();
  }

  @Override public RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {
    for (RelDataTypeField f : fields) {
      if (f.getName().equalsIgnoreCase(fieldName)) {
        return f;
      }
    }

    Table table = (Table)this.table;

    NamePath namePath = NamePath.parse(fieldName);

    if (namePath.getPrefix().isPresent()) {
      table = table.walk(namePath.getPrefix().get())
          .orElseThrow(()->new RuntimeException(String.format("Could not find table %s", namePath.getPrefix().get())));

    }
//    NamePath.parse();
    Field field = table.getField(namePath.getLast());
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