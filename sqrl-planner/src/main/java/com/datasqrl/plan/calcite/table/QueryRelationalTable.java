package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.stats.TableStatistic;
import com.datasqrl.name.Name;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A relational table that is defined by the user query in the SQRL script.
 *
 * This is a physical relation that gets materialized in the write DAG or computed in the read DAG.
 */
@Getter
public class QueryRelationalTable extends AbstractRelationalTable {

  @NonNull
  private final TableType type;
  @NonNull
  private final TimestampHolder.Base timestamp;
  @NonNull
  private final int numPrimaryKeys;

  protected RelNode relNode;
  /* Additional operators at the root of the relNode logical plan that we want to pull-up as much as possible
  and execute in the database because they are expensive or impossible to execute in a stream
   */
  private final PullupOperator.Container pullups;

  private final TableStatistic tableStatistic;

  /*
   The materialization strategy for this table as determined by the optimizer. This is used when cutting
   the DAG and expanding the RelNodes.
   */
  private final ExecutionStage execution;


  public QueryRelationalTable(@NonNull Name rootTableId, @NonNull TableType type,
                              RelNode relNode, PullupOperator.Container pullups,
                              @NonNull TimestampHolder.Base timestamp,
                              @NonNull int numPrimaryKeys,
                              @NonNull TableStatistic stats,
                              @NonNull ExecutionStage execution) {
    super(rootTableId);
    this.type = type;
    this.timestamp = timestamp;
    this.relNode = relNode;
    this.pullups = pullups;
    this.numPrimaryKeys = numPrimaryKeys;
    this.tableStatistic = stats;
    this.execution = execution;
  }

  public RelNode getRelNode() {
    Preconditions.checkState(relNode!=null,"Not yet initialized");
    return relNode;
  }

  public int getNumColumns() {
    return relNode.getRowType().getFieldCount();
  }

  public void updateRelNode(@NonNull RelNode relNode) {
    this.relNode = relNode;
  }

  public int addInlinedColumn(AddedColumn.Simple column, Supplier<RelBuilder> relBuilderFactory,
                               Optional<Integer> timestampScore) {
    int index = getNumColumns();
    this.relNode = column.appendTo(relBuilderFactory.get().push(relNode)).build();
    //Check if this adds a timestamp candidate
    if (timestampScore.isPresent() && !timestamp.isCandidatesLocked()) {
      timestamp.addCandidate(index, timestampScore.get());
    }
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

  @Override
  public RelDataType getRowType() {
    return relNode.getRowType();
  }

  public RelDataTypeField getField(FieldIndexPath path) {
    return getField(path, getRowType());
  }

  public Statistic getStatistic() {
    if (tableStatistic.isUnknown()) return Statistics.UNKNOWN;
    ImmutableBitSet key = ImmutableBitSet.of(ContiguousSet.closedOpen(0,numPrimaryKeys));
    return Statistics.of(tableStatistic.getRowCount(), List.of(key));
  }


  @Override
  public List<String> getPrimaryKeyNames() {
    throw new UnsupportedOperationException();
  }
}
