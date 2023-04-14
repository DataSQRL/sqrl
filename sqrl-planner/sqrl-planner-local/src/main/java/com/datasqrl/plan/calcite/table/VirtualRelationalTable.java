/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.stats.TableStatistic;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.calcite.util.IndexMap;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.TableVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * A relational table that represents the relational equivalent of a
 * {@link com.datasqrl.schema.SQRLTable}.
 * <p>
 * While the SQRLTable represents the logical table that a user defines in an SQRL script, the
 * associated {@link VirtualRelationalTable} represents the relational representation of that same
 * table (i.e. with primary keys, relational data type, etc). The transpiler manages the mapping
 * between the two and updates both according to the script statements. Specifically, relationships
 * on SQRLTables are converted to JOINs between virtual tables.
 */
@Getter
public abstract class VirtualRelationalTable extends AbstractRelationalTable {

  protected final int numLocalPks;
  @NonNull
  protected final List<AddedColumn> addedColumns = new ArrayList<>();
  /**
   * The data type for the (possibly shredded) table represented by this virtual table
   */
  @NonNull
  protected RelDataType rowType;
  /**
   * The row type of the underlying {@link ScriptRelationalTable} at the shredding level of this
   * virtual table including any nested relations. This is distinct from the rowType of the virtual
   * table which is padded with parent primary keys and does not contain nested relations.
   */
  @NonNull
  protected RelDataType queryRowType;

  @Getter
  @Setter
  private SQRLTable sqrlTable;

  protected VirtualRelationalTable(Name nameId, @NonNull RelDataType rowType,
      @NonNull RelDataType queryRowType, int numLocalPks) {
    super(nameId);
    this.rowType = rowType;
    this.queryRowType = queryRowType;
    this.numLocalPks = numLocalPks;
  }

  public abstract boolean isRoot();

  public abstract VirtualRelationalTable.Root getRoot();

  public void addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory,
      Optional<Integer> timestampScore) {
    if (isRoot() && column instanceof AddedColumn.Simple && addedColumns.isEmpty()) {
      //We can inline this column on the parent table
      ((VirtualRelationalTable.Root) this).getBase().addInlinedColumn((AddedColumn.Simple) column,
          typeFactory, timestampScore);
    } else {
      Preconditions.checkArgument(!isRoot(),"Complex columns not yet supported");
      addedColumns.add(column);
    }
    //Update the row types
    if (!CalciteUtil.isNestedTable(column.getDataType())) {
      rowType = column.appendTo(rowType, typeFactory);
    }
    queryRowType = column.appendTo(queryRowType, typeFactory);
  }

  public abstract int getNumParentPks();

  public int getNumPrimaryKeys() {
    return getNumParentPks() + numLocalPks;
  }

  public List<String> getPrimaryKeyNames() {
    List<String> pkNames = new ArrayList<>(getNumPrimaryKeys());
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < getNumPrimaryKeys(); i++) {
      pkNames.add(i, fields.get(i).getName());
    }
    return pkNames;
  }

  public int getNumColumns() {
    return rowType.getFieldCount();
  }

  public int getNumQueryColumns() {
    return queryRowType.getFieldCount();
  }

  private transient Statistic statistic = null;

  @Override
  public Statistic getStatistic() {
    if (statistic == null) {
      TableStatistic tblStats = getTableStatistic();
      Statistic stats;
      if (tblStats.isUnknown()) {
        //TODO: log warning;
        stats = Statistics.UNKNOWN;
      } else {
        ImmutableBitSet primaryKey = ImmutableBitSet.of(
            ContiguousSet.closedOpen(0, getNumPrimaryKeys()));
        stats = Statistics.of(tblStats.getRowCount(), List.of(primaryKey));
        statistic = stats;
      }
      return stats;
    }
    return statistic;
  }


  public abstract TableStatistic getTableStatistic();


  @Getter
  public static class Root extends VirtualRelationalTable {

    @NonNull
    final ScriptRelationalTable base;

    protected Root(Name nameId, @NonNull RelDataType rowType, @NonNull ScriptRelationalTable base) {
      super(nameId, rowType, base.getRowType(), base.getNumPrimaryKeys());
      this.base = base;
    }


    public IndexMap mapQueryTable() {
      Map<Integer, Integer> mapping = new HashMap<>();
      int vTablePos = 0;
      for (int i = 0; i < queryRowType.getFieldCount(); i++) {
        RelDataTypeField field = queryRowType.getFieldList().get(i);
        if (!CalciteUtil.isNestedTable(field.getType())) {
          mapping.put(i, vTablePos++);
        }
      }
      assert vTablePos == getNumColumns();
      return IndexMap.of(mapping);
    }

    @Override
    public boolean isRoot() {
      return true;
    }

    @Override
    public Root getRoot() {
      return this;
    }

    @Override
    public int getNumParentPks() {
      return 0;
    }

    @Override
    public TableStatistic getTableStatistic() {
      return base.getTableStatistic();
    }

    public <R, C> R accept(RootVirtualTableVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }

    public interface RootVirtualTableVisitor<R, C> extends TableVisitor<R, C> {
      R visit(Root table, C context);
    }
  }

  @Getter
  public static class Child extends VirtualRelationalTable {

    @NonNull
    final VirtualRelationalTable parent;
    final int shredIndex;

    private Child(Name nameId, @NonNull RelDataType rowType, @NonNull VirtualRelationalTable parent,
        int shredIndex, RelDataType queryDataType, int numLocalPks) {
      super(nameId, rowType, queryDataType, numLocalPks);
      this.parent = parent;
      this.shredIndex = shredIndex;
    }

    public static Child of(Name nameId, @NonNull RelDataType rowType,
        @NonNull VirtualRelationalTable parent, @NonNull String shredFieldName) {
      RelDataTypeField shredField = parent.getQueryRowType().getField(shredFieldName, true, false);
      Preconditions.checkArgument(shredField != null);
      RelDataType type = shredField.getType();
      Preconditions.checkArgument(CalciteUtil.isNestedTable(type));
      //We currently make the hard-coded assumption that children have at most one local primary
      int numLocalPks = CalciteUtil.getArrayElementType(type).isPresent() ? 1 : 0;
      //unwrap if type is in array
      type = CalciteUtil.getArrayElementType(type).orElse(type);
      Child child = new Child(nameId, rowType, parent, shredField.getIndex(), type, numLocalPks);
      return child;
    }

    @Override
    public boolean isRoot() {
      return false;
    }

    @Override
    public Root getRoot() {
      return parent.getRoot();
    }

    @Override
    public int getNumParentPks() {
      return parent.getNumPrimaryKeys();
    }

    @Override
    public TableStatistic getTableStatistic() {
      if (numLocalPks > 0) {
        return parent.getTableStatistic().nested();
      } else {
        return parent.getTableStatistic();
      }
    }

    public void appendTimestampColumn(@NonNull RelDataTypeFactory typeFactory) {
      ScriptRelationalTable base = getRoot().getBase();
      int timestampIdx = base.getTimestamp().getTimestampCandidate().getIndex();
      RelDataTypeField timestampField = base.getRowType().getFieldList().get(timestampIdx);
      rowType = CalciteUtil.appendField(rowType, timestampField.getName(),
          timestampField.getType(), typeFactory);
    }

    public <R, C> R accept(ChildVirtualTableVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
    public interface ChildVirtualTableVisitor<R, C> extends TableVisitor<R, C> {
      R visit(Child table, C context);
    }
  }

}
