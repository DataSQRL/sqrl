/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.rules.SQRLConverter;
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
public abstract class PhysicalRelationalTable extends ScriptRelationalTable implements PhysicalTable {

  @NonNull
  protected final Name tableName;
  @NonNull
  protected final TableType type;
  @NonNull
  protected final int numPrimaryKeys;
  @NonNull
  protected TimestampInference timestamp;
  @Setter
  protected RelNode plannedRelNode;
  @Setter
  protected Optional<ExecutionStage> assignedStage = Optional.empty();
  @NonNull
  protected final TableStatistic tableStatistic;

  public PhysicalRelationalTable( Name rootTableId, @NonNull Name tableName, @NonNull TableType type,
                                  RelDataType rowType, @NonNull TimestampInference timestamp,
                                 @NonNull int numPrimaryKeys, @NonNull TableStatistic tableStatistic) {
    super(rootTableId, rowType);
    this.tableName = tableName;
    this.type = type;
    this.timestamp = timestamp;
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

  @Override
  public boolean isRoot() {
    return true;
  }

  @Override
  public PhysicalRelationalTable getRoot() {
    return this;
  }

  public Statistic getStatistic() {
    if (tableStatistic.isUnknown()) {
      return Statistics.UNKNOWN;
    }
    ImmutableBitSet key = ImmutableBitSet.of(ContiguousSet.closedOpen(0, numPrimaryKeys));
    return Statistics.of(tableStatistic.getRowCount(), List.of(key));
  }
}
