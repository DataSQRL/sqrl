package org.apache.calcite.schema;

import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.RelDataTypeFieldFactory;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.SqrlRelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * A dynamically expanding table. All paths that can be specified as columns are resolved as they
 * are referenced.
 *
 * This table must maintain state between two phases of the calcite process. Expanded columns gets
 * constructed during Validation and then must maintain the same positions
 */
@Getter
public class SqrlCalciteTable
    extends RelDataTypeImpl
    implements CustomColumnResolvingTable {

  private final Table sqrlTable;
  private final Table baseTable;
  private final FieldPath fieldPath;
  private final List<FieldPath> addedFields = new ArrayList();

  private final boolean isContext;
  private final List<RelDataTypeField> fields;
  private final String name;
  private final boolean isPath;
  Relationship relationship;

  private final RelDataTypeFieldFactory fieldFactory;

  public SqrlCalciteTable(Table sqrlTable, Table baseTable,
      FieldPath fieldPath, RelDataTypeFieldFactory fieldFactory, boolean isContext,
      List<RelDataTypeField> fields, String name,
      boolean isPath, Relationship relationship) {
    this.sqrlTable = sqrlTable;
    this.baseTable = baseTable;
    this.fieldPath = fieldPath;
    this.fieldFactory = fieldFactory;
    this.isContext = isContext;
    this.fields = fields;
    this.name = name;
    this.isPath = isPath;
    this.relationship = relationship;
    computeDigest();
  }


  /**
   * This only gets call once in the validation process. We need to be able to modify
   * its digest when new columns are discovered and added.
   */
  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return this;
  }

  /**
   * This -attempts- to resolve a column, empty list if it is not found. Empty list indicates
   * if the column does not exist.
   *
   * Calcite will look sometimes look for aliased columns.
   * For pathed relations, the order is that it will look for the first field
   * in the path to determine if it is a field or a struct. If it is a struct,
   *
   */
  @Override
  public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
      RelDataTypeFactory relDataTypeFactory, List<String> list) {
    //TODO: investigate why the fieldname may be empty
    if (list.size() == 0) {
      throw new RuntimeException("?");
    }

    String fieldName = String.join(".", list);
    NamePath namePath = NamePath.parse(fieldName);

    Optional<FieldPath> fieldPath;
    //check if resolvable by this table
    fieldPath = sqrlTable.getField(namePath);
    if (fieldPath.isEmpty()) {
      return List.of();
    }

    //Check for fields already defined
    Optional<RelDataTypeField> existingField = getField(fieldPath.get());
    if (existingField.isPresent()) {
      return List.of(
          Pair.of(existingField.get(), List.of())
      );
    }

    //Consume the entire field
    RelDataTypeField resolvedField = addField(fieldPath.get());
    Preconditions.checkNotNull(resolvedField, "Field should not be null %s", namePath);
    return List.of(
        Pair.of(resolvedField, List.of())
    );
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
      CalciteConnectionConfig calciteConnectionConfig) {
    return true;
  }

  @Override
  public int getFieldCount() {
    return fields.size();
  }

  @Override
  public RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {

    return fields.stream()
        .filter(f->f.getName().equalsIgnoreCase(fieldName))
        .findFirst()
        .orElse(null);
  }

  public RelDataTypeField addField(FieldPath fieldPath) {
    RelDataTypeField field = fieldFactory.create(fieldPath, this.fields.size());
    this.fields.add(field);
    this.addedFields.add(fieldPath);
    computeDigest();

    return field;
  }

  public Optional<RelDataTypeField> getField(FieldPath fieldPath) {
    return fields.stream()
        .filter(f->((SqrlRelDataTypeField)f).getPath().getName().equalsIgnoreCase(fieldPath.getName()))
        .findFirst();
  }
  @Override
  public List<RelDataTypeField> getFieldList() {
    return fields;
  }

  @Override
  public List<String> getFieldNames() {
    return fields.stream().map(e->((SqrlRelDataTypeField)e).getPath().getName()).collect(Collectors.toList());
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.<SqlTypeName>emptyList());
  }

  /**
   * Called when computing the digest
   */
  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(DynamicRecordRow")
        .append(getFieldNames())
        .append(")");
  }

  @Override
  public boolean isStruct() {
    return true;
  }

  @Override public RelDataTypeFamily getFamily() {
    return getSqlTypeName().getFamily();
  }

  @Override
  public StructKind getStructKind() {
    return StructKind.PEEK_FIELDS;
  }

  public Relationship getRelationship() {
    return this.relationship;
  }
}