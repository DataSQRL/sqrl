package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.table.LogicalNestedTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.TableVisitor;
import com.datasqrl.util.StreamUtil;
import java.util.ArrayList;
import java.util.HashMap;
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
    //Create all tables, including shadowed

    List<SqrlTableMacro> tableFunctions = schema.getTableFunctions();
    Map<NamePath, SQRLTable> tables = new HashMap<>();
    for (SqrlTableMacro macro : tableFunctions) {
      if (macro instanceof com.datasqrl.schema.Relationship &&
          (((com.datasqrl.schema.Relationship)macro).getJoinType() != JoinType.CHILD)) continue;
      SQRLTable table = tables.get(macro.getAbsolutePath());
      if (table == null) {
        table = createSqrlTable(macro, schema);
        tables.put(macro.getAbsolutePath(), table);
      }
    }

    //Add relationships
    for (SqrlTableMacro macro : tableFunctions) {
      if (macro instanceof com.datasqrl.schema.Relationship) {
        com.datasqrl.schema.Relationship rel = (com.datasqrl.schema.Relationship) macro;
        SQRLTable fromTable = tables.get(rel.getFullPath().popLast());
        NamePath toNamePath = schema.getPathToAbsolutePathMap()
            .get(rel.getFullPath());
        SQRLTable toTable = tables.get(toNamePath);

        Relationship relationship = new Relationship(fromTable, toTable, macro, rel.getName(),
            rel.getFullPath().toStringList(),
            rel.getParameters(), rel.getMultiplicity(), rel.getJoinType());
        fromTable.fields.add(relationship);
      }
    }

    this.tableMap = tables;
  }

  private SQRLTable createSqrlTable(SqrlTableMacro macro, SqrlSchema schema) {
    String tableName = schema.getPathToSysTableMap().get(macro.getAbsolutePath());
    Table table = schema.getTable(tableName, false).getTable();

    SQRLTable sqrlTable = new SQRLTable(macro.getAbsolutePath(), table, macro);
    List<RelDataTypeField> fieldList = macro.getRowType().getFieldList();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      if (field.getType() instanceof ArraySqlType || field.getType() instanceof ObjectSqlType) {
        continue;
      }

      sqrlTable.fields.add(new Column(field.getType(),
          Name.system(field.getName()),
          field.getName(),
          isLocalPrimaryKey(table, i)));
    }
    return sqrlTable;
  }

  private boolean isLocalPrimaryKey(Table table, int i) {
    if (table instanceof LogicalNestedTable) {
      LogicalNestedTable nestedTable = (LogicalNestedTable) table;
      if (i < nestedTable.getNumPrimaryKeys() - nestedTable.getNumLocalPks()) {
        return false;
      } else if (i < nestedTable.getNumPrimaryKeys()) {
        return true;
      }
    } else if (table instanceof ScriptRelationalTable) {
      ScriptRelationalTable relTable = (ScriptRelationalTable) table;
      if (i < relTable.getNumPrimaryKeys()) {
        return true;
      }
    }

    return false;
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
    public abstract Name getName();

    public abstract <R, C> R accept(FieldVisitor<R, C> visitor, C context);
  }
  @AllArgsConstructor
  @Getter
  public static class Column extends Field {
    RelDataType type;
    Name name;
    String vtName;
    boolean primaryKey;

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
    SqrlTableMacro macro;
    Name name;
    List<String> namePath;
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
    protected final SqrlTableMacro tableMacro;
    protected final List<Field> fields = new ArrayList<>();
    protected final List<FunctionParameter> params = new ArrayList<>();

    protected final List<SQRLTable> isTypeOf = new ArrayList<>();

    public String getNameId() {
      return ((ModifiableTable) getVt()).getNameId();
    }

    public String getName() {
      return path.getLast().getDisplay();
    }

    public Optional<Field> getField(Name name) {
      return fields.stream()
          .filter(f->f.getName().equals(name))
          .findAny();
    }

    public List<Column> getVisibleColumns() {
      return getColumns(true);
    }

    public List<Column> getPrimaryKeys() {
      return getFieldsByClass(Column.class, true)
          .stream()
          .filter(Column::isPrimaryKey)
          .collect(Collectors.toList());
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
                          ? !f.getName().getDisplay().startsWith(ReservedName.HIDDEN_PREFIX)
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
