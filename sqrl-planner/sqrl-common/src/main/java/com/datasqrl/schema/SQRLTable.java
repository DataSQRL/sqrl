/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
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
@AllArgsConstructor
public class SQRLTable implements ModifiableSqrlTable {
  protected final NamePath path;
  protected Table relOptTable;
  protected final FieldList fields = new FieldList();

  protected final List<SQRLTable> isTypeOf;

  @Override
  public void addColumn(String name, RexNode column, RelDataTypeFactory typeFactory) {
    //add column logic
  }

  @Override
  public SQRLTable getSqrlTable() {
    return this;
  }

  @Override
  public String getNameId() {
    return ((NamedTable)getVt()).getNameId();
  }

  public Name getName() {
    return path.getLast();
  }

  private int getNextFieldVersion(Name name) {

    return fields.nextVersion(name);
  }

  public Column addColumn(SqrlFramework framework, Name name, Name vtName, boolean visible, RelDataType type) {
    int version = name.getCanonical().split("\\$").length > 1 ?
        Integer.parseInt(name.getCanonical().split("\\$")[1]) : getNextFieldVersion(name);

    Column col = new Column(name, vtName, version,
        visible, type);
    fields.addField(col);
    return col;
  }

//  public Relationship addRelationship(Name name, SQRLTable toTable, JoinType joinType,
//      Multiplicity multiplicity) {
//
//
////    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType,
////        multiplicity, null, null);
////    fields.addField(rel);
////    return rel;
//    return null;
//  }

  public Optional<Field> getField(Name name) {
    return fields.getAccessibleField(name);
  }

  public List<Column> getVisibleColumns() {
    return getColumns(true);
  }

  public List<Column> getColumns(boolean onlyVisible) {
    return StreamUtil.filterByClass(fields.getFields(onlyVisible), Column.class)
        .collect(Collectors.toList());
  }

  public <R, C> R accept(SqrlTableVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public void addRelationship(Relationship relationship) {
    this.fields.addField(relationship);
  }

  public void setVtTable(Table relOptTable) {
    this.relOptTable = relOptTable;
  }

  public Table getVt() {
    return relOptTable;
  }

  public interface SqrlTableVisitor<R, C> extends TableVisitor<R, C> {
    R visit(SQRLTable table, C context);
  }
}
