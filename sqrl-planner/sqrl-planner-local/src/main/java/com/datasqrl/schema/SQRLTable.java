/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.table.AddedColumn;
import com.datasqrl.plan.table.AddedColumn.Simple;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.TableFunctionArgument;

/**
 * A {@link SQRLTable} represents a logical table in the SQRL script which contains fields that are
 * either columns or relationships.
 * <p>
 * Note, that SQRLTables are always flat and hierarchical data is represented as multiple SQRLTables
 * with parent-child relationships between them.
 */
@Getter
public class SQRLTable implements Table, org.apache.calcite.schema.Schema, ScannableTable {

  @NonNull
  NamePath path;
  @NonNull
  final FieldList fields = new FieldList();

  private RelDataType fullDataType;
  private Optional<SQRLTable> parent;
  private Optional<List<TableFunctionArgument>> tableArguments = Optional.empty();
  private VirtualRelationalTable vt;

  public SQRLTable() {

  }

  public SQRLTable(RelDataType fullDataType) {
    this.fullDataType = fullDataType;
    this.parent = Optional.empty();
  }

  public SQRLTable(RelDataType fullDataType, SQRLTable parent) {
    this.fullDataType = fullDataType;
    this.parent = Optional.of(parent);
  }

  public SQRLTable(@NonNull NamePath path) {
    this.path = path;
  }

  public Name getName() {
    return path.getLast();
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("Table[path=").append(path).append("]{\n");
    for (Field f : fields.getAccessibleFields()) {
      s.append("\t").append(f).append("\n");
    }
    s.append("}");
    return s.toString();
  }

  private int getNextFieldVersion(Name name) {
    return fields.nextVersion(name);
  }

  public Column addColumn(Name name, Name vtName, boolean visible, RelDataType type) {
    Column col = new Column(name, vtName, getNextFieldVersion(name), visible, type);
    fields.addField(col);
    return col;
  }

  public Relationship addRelationship(Name name, SQRLTable toTable, JoinType joinType,
      Multiplicity multiplicity, Optional<SqrlJoinDeclarationSpec> join) {
    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType,
        multiplicity,
        join);
    fields.addField(rel);
    return rel;
  }

  public void setVT(VirtualRelationalTable vt) {
    this.vt = vt;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return vt.getRowType();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TYPED_TABLE;
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
      CalciteConnectionConfig calciteConnectionConfig) {
    return false;
  }

  @Override
  public Table getTable(String s) {
    Optional<SQRLTable> rel = this.getAllRelationships()
        .filter(e -> e.getName().getCanonical().equalsIgnoreCase(s))
        .map(r -> r.getToTable())
        .findAny();

    return rel.orElse(null);
  }

  @Override
  public Set<String> getTableNames() {

    return this.getAllRelationships().map(s -> s.getName().getDisplay())
        .collect(Collectors.toSet());
//    Set<String> names = this.dataType.getFieldList().stream()
//        .filter(f->
//            f.getType() instanceof ArraySqlType || f.getType() instanceof RelRecordType)
//        .map(f->f.getName())
//        .collect(Collectors.toSet());
//    parent.map(p->names.add(ReservedName.PARENT.getCanonical()));

//    return names;
  }

  @Override
  public RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return null;
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return null;
  }

  @Override
  public Set<String> getFunctionNames() {
    return null;
  }

  @Override
  public org.apache.calcite.schema.Schema getSubSchema(String s) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return null;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public org.apache.calcite.schema.Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }

  public Optional<Field> getField(Name name) {
    return getField(name, false);
  }

  public Optional<Field> getField(Name name, boolean fullColumn) {
    return fields.getAccessibleField(name);
  }

  public Optional<SQRLTable> walkTable(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty() || !(field.get() instanceof Relationship)) {
      return Optional.empty();
    }
    Relationship rel = (Relationship) field.get();
    SQRLTable target = rel.getToTable();
    return target.walkTable(namePath.popFirst());
  }

  public Stream<Relationship> getAllRelationships() {
    return StreamUtil.filterByClass(fields.getFields(true), Relationship.class);
  }

//  public Optional<SQRLTable> getParent() {
//    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getToTable).findFirst();
//  }

  public Collection<SQRLTable> getChildren() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.CHILD)
        .map(Relationship::getToTable).collect(Collectors.toList());
  }

  public Optional<SQRLTable> getParent() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT)
        .map(Relationship::getFromTable).findFirst();
  }

  public List<Column> getVisibleColumns() {
    return getColumns(true);
  }

  public List<Column> getColumns(boolean onlyVisible) {
    return StreamUtil.filterByClass(fields.getFields(onlyVisible), Column.class)
        .collect(Collectors.toList());
  }

  public Optional<Field> getField(NamePath names) {
    if (names.isEmpty()) {
      return Optional.empty();
    }
    SQRLTable t = this;
    for (Name n : names.popLast()) {
      Optional<Field> field = t.getField(n);
      if (field.isPresent() && field.get() instanceof Relationship) {
        t = ((Relationship) field.get()).getToTable();
      } else {
        return Optional.empty();
      }
    }
    return t.getField(names.getLast());
  }

  public List<Field> walkField(NamePath path) {
    return walkField(List.of(path.getNames()));
  }

  public List<Field> walkField(List<Name> names) {
    List<Field> fields = new ArrayList<>();
    SQRLTable t = this;
    for (Name n : names) {
      Field field = t.getField(n).get();
      fields.add(field);
      if (field instanceof Relationship) {
        t = ((Relationship) field).getToTable();
      }
    }
    return fields;
  }

  public <R, C> R accept(SqrlTableVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public void addColumn(Name name, RelNode relNode, boolean lockTimestamp) {
    checkColumnPreconditions(name);

//    NamePath namePath = toNamePath(env, op.getStatement().getNamePath());
//    Name name = namePath.getLast();
    RelDataTypeField relDataTypeField = relNode.getRowType().getFieldList()
        .get(relNode.getRowType().getFieldCount() - 1);
    Name vtName = uniquifyColumnName(name, this);

    VirtualRelationalTable vtable = getVt();
    Optional<Integer> timestampScore = CalciteTableFactory.getTimestampScore(
        name, relDataTypeField.getType());

    Preconditions.checkState(
        relNode instanceof Project && relNode.getInput(0) instanceof TableScan,
        "Complex columns not yet supported");
    Project project = (Project) relNode;

    AddedColumn addedColumn = new Simple(name.getCanonical(), project.getProjects().get(project.getProjects().size() - 1));
    addedColumn.setNameId(vtName.getCanonical());

    addColumnToVirtualTable(addedColumn, vtable, timestampScore);
    if (lockTimestamp) {
      lockTimestampScore(vtable);
    }
    addColumnToSQRLTable(name, addedColumn, vtName);
  }

  private void addColumnToVirtualTable(AddedColumn addedColumn, VirtualRelationalTable vtable, Optional<Integer> timestampScore) {
    vtable.addColumn(addedColumn, TypeFactory.getTypeFactory(), timestampScore);
  }

  private void lockTimestampScore(VirtualRelationalTable vtable) {
    Preconditions.checkState(vtable.isRoot() && vtable.getAddedColumns().isEmpty());
    ScriptRelationalTable baseTbl = ((VirtualRelationalTable.Root) vtable).getBase();
    baseTbl.getTimestamp()
        .getCandidateByIndex(baseTbl.getNumColumns() - 1) //Timestamp must be last column
        .lockTimestamp();
  }

  private void addColumnToSQRLTable(Name name, AddedColumn c, Name vtName) {
    addColumn(name, vtName, true, c.getDataType());
  }
  private void checkColumnPreconditions(Name name) {
    if (getField(name).isPresent()) {
      if (getField(name).get() instanceof Relationship) {
        throw new RuntimeException("Cannot shadow relationship: " + name);
      }
//      checkState(!(table.getField(toNamePath(env, op.statement.getNamePath()).getLast())
//              .get() instanceof Relationship),
//          ErrorCode.CANNOT_SHADOW_RELATIONSHIP, op.statement);
    }
  }

  private Name uniquifyColumnName(Name name, SQRLTable table) {
    if (table.getField(name).isPresent()) {
      String newName = org.apache.calcite.sql.validate.SqlValidatorUtil.uniquify(
          name.getCanonical(),
          new HashSet<>(table.getVt().getRowType().getFieldNames()),
          //Renamed columns to names the user cannot address to prevent collisions
          (original, attempt, size) -> original + "$" + attempt);
      return Name.system(newName);
    }

    return name;
  }
  public interface SqrlTableVisitor<R, C> extends TableVisitor<R, C> {
    R visit(SQRLTable table, C context);
  }
}
