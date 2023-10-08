package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.TableVisitor;
import com.datasqrl.util.StreamUtil;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.ObjectSqlType;

public class SqrlSchemaForInference {

  private final Map<NamePath, SQRLTable> tableMap;

  public SqrlSchemaForInference(SqrlSchema schema) {
    Map<NamePath, SQRLTable> tableMap = new LinkedHashMap<>();
    //Most recent tables
    for (Entry<NamePath, String> entry : schema.getPathToTableMap().entrySet()) {
      Table table = schema.getTable(entry.getValue(), false)
          .getTable();

      SQRLTable sqrlTable = createSqrlTable(table, entry.getKey());
      tableMap.put(entry.getKey(), sqrlTable);
    }

    for (Entry<NamePath, com.datasqrl.schema.Relationship> rel : schema.getPathToRelationshipMap().entrySet()) {
      com.datasqrl.schema.Relationship r = rel.getValue();
      SQRLTable from = tableMap.get(r.getFromTable());
      SQRLTable to = tableMap.get(r.getToTable());

      Relationship relationship = new Relationship(from, to, r.getName(), r.getParameters(),
          r.getMultiplicity(), r.getJoinType());
      NamePath path = rel.getKey().popLast();
      tableMap.get(path).fields
          .add(relationship);
    }

    this.tableMap = tableMap;
  }

  private SQRLTable createSqrlTable(Table table, NamePath path) {
    SQRLTable sqrlTable = new SQRLTable(path, table, List.of());
    List<RelDataTypeField> fieldList = table.getRowType(null).getFieldList();
    for (RelDataTypeField field : fieldList) {
      if (field.getType() instanceof ArraySqlType || field.getType() instanceof ObjectSqlType) {
        continue;
      }

      sqrlTable.fields.add(new Column(field.getType(),
          Name.system(field.getName()), Name.system(field.getName()),
          field.getName()));
    }
    return sqrlTable;
  }

  public List<SQRLTable> getRootTables() {
    return tableMap.entrySet().stream()
        .filter(f->f.getKey().size() == 1)
        .map(Entry::getValue)
        .collect(Collectors.toList());
  }

  public SQRLTable getRootSqrlTable(String fieldName) {
    return  tableMap.entrySet().stream()
        .filter(f->f.getKey().size() == 1)
        .filter(f->f.getKey().get(0).equals(Name.system(fieldName)))
        .map(Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  public List<SQRLTable> getAllTables() {
    return new ArrayList<>(tableMap.values());
  }
  public static abstract class Field {
    public abstract Name getId();

    public abstract <R, C> R accept(FieldVisitor<R, C> visitor, C context);
  }
  @AllArgsConstructor
  @Getter
  public static class Column extends Field {
    RelDataType type;
    Name name;
    Name id;
    String vtName;

    @Override
    public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @AllArgsConstructor
  @Getter
  public static class Relationship extends Field {
    SQRLTable fromTable;
    SQRLTable toTable;
    Name id;
    List<FunctionParameter> parameters;
    Multiplicity multiplicity;
    JoinType joinType;
    @Override
    public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }
  @AllArgsConstructor
  @Getter
  public static class SQRLTable {

    protected final NamePath path;
    protected final Table relOptTable;
    protected final List<Field> fields = new ArrayList<>();

    protected final List<SQRLTable> isTypeOf;

    public String getNameId() {
      return ((ModifiableTable) getVt()).getNameId();
    }

    public String getName() {
      return path.getLast().getDisplay();
    }

    public Optional<Field> getField(Name name) {
      return fields.stream()
          .filter(f->f.getId().equals(name))
          .findAny();
    }

    public List<Column> getVisibleColumns() {
      return getColumns(true);
    }

    public List<Column> getColumns(boolean onlyVisible) {
      return getFieldsByClass(Column.class, onlyVisible);
    }

    public List<Field> getFields(boolean onlyVisible) {
      return getFieldsByClass(Field.class, onlyVisible);
    }

    public <T extends Field> List<T> getFieldsByClass(Class<T> clazz, boolean onlyVisible) {
      return StreamUtil.filterByClass(fields.stream()
                  .filter(f->
                      onlyVisible
                          ? !f.getId().getDisplay().startsWith(ReservedName.HIDDEN_PREFIX)
                          : true),
              clazz)
          .collect(Collectors.toList());
    }

    public <R, C> R accept(SQRLTable.SqrlTableVisitor<R, C> visitor,
        C context) {
      return visitor.visit(this, context);
    }

    public Table getVt() {
      return relOptTable;
    }

    public List<SQRLTable> getIsTypeOf() {
      return List.of();
    }

    public interface SqrlTableVisitor<R, C> extends TableVisitor<R, C> {

      R visit(SQRLTable table, C context);
    }
  }

  public static interface FieldVisitor<R, C> {
    R visit(Column column, C context);
    R visit(Relationship column, C context);
  }

  public <R, C> R accept(CalciteSchemaVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
  public interface CalciteSchemaVisitor<R, C> {

    R visit(SqrlSchemaForInference schema, C context);
  }
}
