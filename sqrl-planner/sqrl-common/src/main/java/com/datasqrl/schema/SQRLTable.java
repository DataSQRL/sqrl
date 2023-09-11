/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.StreamUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;

/**
 * A {@link SQRLTable} represents a logical table in the SQRL script which contains fields that are
 * either columns or relationships.
 * <p>
 * Note, that SQRLTables are always flat and hierarchical data is represented as multiple SQRLTables
 * with parent-child relationships between them.
 */
@Getter
@ToString
public class SQRLTable {
  final NamePath path;
  private final Table vTable;
  private final int numPrimaryKeys;
  final FieldList fields = new FieldList();

  public SQRLTable(@NonNull NamePath path, Table vTable, int numPrimaryKeys) {
    this.path = path;
    this.vTable = vTable;
    this.numPrimaryKeys = numPrimaryKeys;
  }

  public Name getName() {
    return path.getLast();
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
      Multiplicity multiplicity) {
    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType,
        multiplicity);
    fields.addField(rel);
    return rel;
  }

  public Optional<Field> getField(Name name) {
    return getField(name, false);
  }

  public Optional<Field> getField(Name name, boolean fullColumn) {
    return fields.getAccessibleField(name);
  }

  public Stream<Relationship> getAllRelationships() {
    return StreamUtil.filterByClass(fields.getFields(true), Relationship.class);
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

  public <R, C> R accept(SqrlTableVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public int getNumPrimaryKeys() {
    return numPrimaryKeys;
  }

  public Table getVt() {
    return vTable;
  }

  public interface SqrlTableVisitor<R, C> extends TableVisitor<R, C> {
    R visit(SQRLTable table, C context);
  }
}
