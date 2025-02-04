/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.PullupOperator.Container;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A relational table that is defined by the user query in the SQRL script.
 * <p>
 * This is a physical relation that gets materialized in the write DAG or computed in the read DAG.
 */
@Getter
public abstract class PhysicalRelationalTable extends ScriptRelationalTable implements PhysicalTable {

  @NonNull
  protected final NamePath tablePath;
  @NonNull
  protected final TableType type;
  @NonNull
  protected final PrimaryKey primaryKey;
  @NonNull
  protected Timestamps timestamp;
  @NonNull
  protected final TableStatistic tableStatistic;

  protected PullupOperator.Container pullups;
  protected RelNode plannedRelNode;

  @Setter
  protected Optional<ExecutionStage> assignedStage = Optional.empty();

  public PhysicalRelationalTable(Name rootTableId, @NonNull NamePath tablePath, @NonNull TableType type,
                                  RelDataType rowType, int numSelects, @NonNull Timestamps timestamp,
                                 @NonNull PrimaryKey primaryKey, @NonNull PullupOperator.Container pullups,
                                 @NonNull TableStatistic tableStatistic) {
    super(rootTableId, rowType, numSelects);
    this.tablePath = tablePath;
    this.type = type;
    this.timestamp = timestamp;
    this.primaryKey = primaryKey;
    this.pullups = pullups;
    this.tableStatistic = tableStatistic;
  }

  public Name getTableName() {
    return tablePath.getLast();
  }

  /* Additional operators at the root of the relNode logical plan that we want to pull-up as much as possible
  and execute in the database because they are expensive or impossible to execute in a stream
  */
  public PullupOperator.Container getPullups() {
    return pullups;
  }

  public abstract Optional<PhysicalRelationalTable> getStreamRoot();

  /**
   *
   * @return the assigned execution stage or empty if no stage has been assigned yet
   */
  @Override
  public Optional<ExecutionStage> getAssignedStage() {
    return assignedStage;
  }

  public abstract List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors);

  public abstract SqrlConverterConfig.SqrlConverterConfigBuilder getBaseConfig();

  public void assignStage(ExecutionStage stage) {
    this.assignedStage = Optional.of(stage);
  }

  public List<OptimizerHint> getOptimizerHints() {
    return List.of();
  }

  @Override
  public RelNode getPlannedRelNode() {
    Preconditions.checkState(plannedRelNode != null, "Table has not been planned");
    return plannedRelNode;
  }

  @Override
  public void setPlannedRelNode(@NonNull SQRLConverter.TablePlan planned) {
    Preconditions.checkArgument(plannedRelNode == null, "Table has already been planned");
    RelNode relNode = planned.getRelNode();
    Preconditions.checkArgument(relNode.getRowType().equalsSansFieldNames(getRowType()), "Row types do not match: %s vs %s", getRowType(), relNode.getRowType());
    Preconditions.checkArgument(relNode.getRowType().getFieldNames().subList(0,getNumSelects()).equals(getRowType().getFieldNames().subList(0,getNumSelects())), "Names do not match");
    this.plannedRelNode = relNode;
    this.pullups = planned.getPullups();
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
    ArrayList<ImmutableBitSet> keys = new ArrayList<>();
    if (primaryKey.isDefined()) {
      keys.add(ImmutableBitSet.of(primaryKey.asArray()));
    }
    return Statistics.of(tableStatistic.getRowCount(), keys);
  }
}
