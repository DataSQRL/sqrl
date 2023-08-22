/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.AddedColumn.Simple;
import com.datasqrl.plan.table.PullupOperator.Container;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * A relational table that is defined by the user query in the SQRL script.
 * <p>
 * This is a physical relation that gets materialized in the write DAG or computed in the read DAG.
 */
@Getter
public abstract class ScriptRelationalTable extends AbstractRelationalTable implements ScriptTable {

  //Name assigned by user - different from nameId which is unique
  @NonNull
  protected final Name tableName;
  @NonNull
  protected final TableType type;
  @NonNull
  protected final TimestampHolder.Base timestamp;
  @NonNull
  protected final int numPrimaryKeys;
  @NonNull
  protected RelDataType rowType;
  //Use only rowtype during planning. Once the final conversion (with stage) is done,
  //set convertedRelNode and use for stitching DAG together
  //the tricky part is keeping track of adding columns which must be added at the END
  //of the rowtype after any columns (like timestamps and sorts) that have been added
  //in the planning
  @Setter
  protected RelNode plannedRelNode;

  private final List<Simple> addedColumns = new ArrayList<>();

  @Setter
  protected Optional<ExecutionStage> assignedStage = Optional.empty();
  @NonNull
  protected final TableStatistic tableStatistic;

  public ScriptRelationalTable(@NonNull Name rootTableId, @NonNull Name tableName,
      @NonNull TableType type, @NonNull RelDataType rowType, @NonNull TimestampHolder.Base timestamp,
      @NonNull int numPrimaryKeys, @NonNull TableStatistic tableStatistic) {
    super(rootTableId);
    this.tableName = tableName;
    this.type = type;
    this.timestamp = timestamp;
    this.rowType = rowType;
    this.numPrimaryKeys = numPrimaryKeys;
    this.tableStatistic = tableStatistic;
  }

  /* Additional operators at the root of the relNode logical plan that we want to pull-up as much as possible
  and execute in the database because they are expensive or impossible to execute in a stream
  */
  public PullupOperator.Container getPullups() {
    return Container.EMPTY;
  }

  /**
   *
   * @return the assigned execution stage or empty if no stage has been assigned yet
   */
  @Override
  public Optional<ExecutionStage> getAssignedStage() {
    return assignedStage;
  }

  public abstract List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors);

  public abstract SQRLConverter.Config.ConfigBuilder getBaseConfig();

  public void assignStage(ExecutionStage stage) {
    this.assignedStage = Optional.of(stage);
  }

  public RelNode getPlannedRelNode() {
    Preconditions.checkState(plannedRelNode != null, "Table has not been planned");
    return plannedRelNode;
  }

  public int getNumColumns() {
    return getRowType().getFieldCount();
  }

  public int addInlinedColumn(AddedColumn.Simple column, @NonNull RelDataTypeFactory typeFactory,
      Optional<Integer> timestampScore) {
    int index = getNumColumns();
    this.rowType = column.appendTo(rowType, typeFactory);
    addedColumns.add(column);
    return index;
  }

  private static RelDataTypeField getField(FieldIndexPath path, RelDataType rowType) {
    Preconditions.checkArgument(path.size() > 0);
    Preconditions.checkArgument(rowType.isStruct(), "Expected relational data type but found: %s",
        rowType);
    int firstIndex = path.get(0);
    Preconditions.checkArgument(firstIndex < rowType.getFieldCount());
    RelDataTypeField field = rowType.getFieldList().get(firstIndex);
    path = path.popFirst();
    if (path.isEmpty()) {
      return field;
    } else {
      return getField(path, field.getType());
    }
  }

  public RelDataTypeField getField(FieldIndexPath path) {
    return getField(path, getRowType());
  }

  public Statistic getStatistic() {
    if (tableStatistic.isUnknown()) {
      return Statistics.UNKNOWN;
    }
    ImmutableBitSet key = ImmutableBitSet.of(ContiguousSet.closedOpen(0, numPrimaryKeys));
    return Statistics.of(tableStatistic.getRowCount(), List.of(key));
  }


  @Override
  public List<String> getPrimaryKeyNames() {
    throw new UnsupportedOperationException();
  }
}
