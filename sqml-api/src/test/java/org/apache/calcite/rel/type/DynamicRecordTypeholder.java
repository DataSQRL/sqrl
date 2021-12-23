package org.apache.calcite.rel.type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

public class DynamicRecordTypeholder extends DynamicRecordType {
  private final RelDataTypeFactory typeFactory;
  private List<RelDataTypeField> fields;

  /** Creates a DynamicRecordTypeImpl. */
  public DynamicRecordTypeholder(RelDataTypeFactory typeFactory,
      List<RelDataTypeField> regFields) {
    this.typeFactory = typeFactory;
    this.fields = regFields;
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
//    if (fields.contains(fieldName)) {
//      return fieldMap.get(fieldName);
//    }

    RelDataTypeField newField;
//    if (fieldName.equalsIgnoreCase("b")) {
//      newField = new RelDataTypeFieldImpl("b", 0, this.typeFactory.createSqlType(SqlTypeName.BOOLEAN));
//      setStruct(false);
//    } else {
      //hierarchical
      newField = new RelDataTypeFieldImpl(fieldName, this.fields.size(), typeFactory.createSqlType(SqlTypeName.BOOLEAN));

//    }
    this.fields.add(newField);

    // If a new field is added, we should re-compute the digest.
    computeDigest();
    return newField;
  }

  @Override public List<String> getFieldNames() {
    return fields.stream().map(e->e.getName()).collect(Collectors.toList());
  }

  @Override public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.<SqlTypeName>emptyList());
  }

  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(DynamicRecordRow").append(")");
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