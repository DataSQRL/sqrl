package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.LogicalPlanImpl.DatasetOrTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

public class DynamicRecordTypeholder extends DynamicRecordType {
  private RelDataTypeFactory typeFactory;
  private List<RelDataTypeField> fields = new ArrayList<>();
  private final DatasetOrTable table;

  /** Creates a DynamicRecordTypeImpl. */
  public DynamicRecordTypeholder(RelDataTypeFactory typeFactory,
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
    RelDataTypeField newField;

    if (fieldName.equalsIgnoreCase("parent.time")) {
      newField = new RelDataTypeFieldImpl(fieldName, this.fields.size(),
          typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    } else if(fieldName.equalsIgnoreCase("quantity")) {
      newField = new RelDataTypeFieldImpl(fieldName, this.fields.size(),
          typeFactory.createSqlType(SqlTypeName.INTEGER));
    } else {
      //todo: We must walk the path
      newField = new RelDataTypeFieldImpl(fieldName, this.fields.size(),
          typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    }

    this.fields.add(newField);

    // If a new field is added, we should re-compute the digest.
    computeDigest();
    return newField;
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