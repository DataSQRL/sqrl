/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
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
@AllArgsConstructor
public class SQRLTable {
  protected final NamePath path;
  protected final Table relOptTable;
  protected final FieldList fields = new FieldList();

  protected final List<SQRLTable> isTypeOf;

  public SQRLTable getSqrlTable() {
    return this;
  }

  public String getNameId() {
    return ((ModifiableTable)getVt()).getNameId();
  }

  public Name getName() {
    return path.getLast();
  }

  public int getNextFieldVersion(Name name) {
    return fields.nextVersion(name);
  }

  public Column addColumn(Name name, boolean visible, RelDataType type) {
    //extract version information from vt column or derive a new version
    int version = name.getCanonical().split("\\$").length > 1 ?
        Integer.parseInt(name.getCanonical().split("\\$")[1]) : getNextFieldVersion(name);

    Column col = new Column(name, version, visible, type);
    fields.addField(col);
    return col;
  }

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

  public Table getVt() {
    return relOptTable;
  }

  public interface SqrlTableVisitor<R, C> extends TableVisitor<R, C> {
    R visit(SQRLTable table, C context);
  }
}
