package com.datasqrl.engine.stream.flink.sql.rules;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.immutables.value.Value;

import com.datasqrl.plan.hints.SlidingAggregationHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.TumbleAggregationHint;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

public class ExpandWindowHintRule extends RelRule<ExpandWindowHintRule.Config>
    implements TransformationRule {

  public ExpandWindowHintRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    var relBuilder = relOptRuleCall.builder();
    LogicalAggregate aggregate = relOptRuleCall.rel(0);
    var input = relOptRuleCall.rel(1);
    Optional<TumbleAggregationHint> tumbleHintOpt = SqrlHint.fromRel(aggregate,
        TumbleAggregationHint.CONSTRUCTOR);
    Optional<SlidingAggregationHint> slideHintOpt = SqrlHint.fromRel(aggregate,
        SlidingAggregationHint.CONSTRUCTOR);
    if (tumbleHintOpt.isEmpty() && slideHintOpt.isEmpty()) {
      return;
    }

    var groupBy = Iterables.getOnlyElement(aggregate.groupSets);
    var aggCalls = aggregate.getAggCallList();

    if (tumbleHintOpt.isPresent() || slideHintOpt.isPresent()) {
      handleWindowedAggregation(relBuilder, tumbleHintOpt, slideHintOpt, groupBy, aggCalls, input);
      relOptRuleCall.transformTo(relBuilder.build());
    }
  }

  private void handleWindowedAggregation(RelBuilder relBuilder,
      Optional<TumbleAggregationHint> tumbleHintOpt, Optional<SlidingAggregationHint> slideHintOpt,
      ImmutableBitSet groupBy, List<AggregateCall> aggCalls, RelNode input) {
    Preconditions.checkArgument(tumbleHintOpt.isPresent() ^ slideHintOpt.isPresent());
    var rexBuilder = getRexBuilder(relBuilder);
    var inputFieldCount = input.getRowType().getFieldCount();
    var inputType = input.getRowType();

    final int timestampIdx = tumbleHintOpt
        .map(tumbleHint -> tumbleHint.getWindowFunctionIdx())
        .orElseGet(() -> slideHintOpt.get().getTimestampIdx());

    tumbleHintOpt.ifPresentOrElse(
        tumbleHint -> handleTumbleWindow(timestampIdx, tumbleHint, input,  relBuilder),
        () -> handleSlidingWindow(timestampIdx, slideHintOpt.get(), input, relBuilder));

    applyWindowedGroupingAndProjection(relBuilder, rexBuilder, inputType, groupBy,
        aggCalls, inputFieldCount, timestampIdx);
  }

  private RexBuilder getRexBuilder(RelBuilder relBuilder) {
    return relBuilder.getRexBuilder();
  }

  public void applyWindowedGroupingAndProjection(RelBuilder relBuilder,
      RexBuilder rexBuilder, RelDataType inputType, ImmutableBitSet groupBy,
      List<AggregateCall> aggCalls, int inputFieldCount, int timestampIdx) {
    //Need to add all 3 window columns that are added to groupBy and then project out all but window_time
    int window_start = inputFieldCount, window_end = inputFieldCount + 1, window_time =
        inputFieldCount + 2;
    List<Integer> groupByIdx = new ArrayList<>();
    List<Integer> projectIdx = new ArrayList<>();
    List<String> projectNames = new ArrayList<>();
    var index = 0;
    var window_time_idx =
        (groupBy.cardinality() - 1) + 3 - 1; //Points at window_time at the end of groupByIdx
    for (int idx : groupBy.asList()) {
      if (idx == timestampIdx) {
        projectIdx.add(window_time_idx);
        projectNames.add(inputType.getFieldNames().get(timestampIdx));
      } else {
        groupByIdx.add(idx);
        projectIdx.add(index++);
        projectNames.add(inputType.getFieldNames().get(idx));
      }
    }
    groupByIdx.add(window_start);
    groupByIdx.add(window_end);
    groupByIdx.add(window_time); //Window_time is new timestamp
    index += 3;
    assert window_time_idx == index - 1;

    for (var i = 0; i < aggCalls.size(); i++) {
      projectIdx.add(index++);
      projectNames.add(null);
    }
    relBuilder.aggregate(relBuilder.groupKey(Ints.toArray(groupByIdx)), aggCalls);
    relBuilder.project(
        projectIdx.stream().map(idx -> rexBuilder.makeInputRef(relBuilder.peek(), idx))
            .collect(Collectors.toList()), projectNames, true);
  }

  private void handleTumbleWindow(int windowTimestampIdx, TumbleAggregationHint tumbleHint, RelNode input, RelBuilder relBuilder) {
    SqlOperator windowFunction = FlinkSqlOperatorTable.TUMBLE;
    relBuilder.push(input);
    long[] windowDef;
    if (tumbleHint.getType() == TumbleAggregationHint.Type.FUNCTION) {
      windowDef = new long[]{tumbleHint.getWindowWidthMs(), tumbleHint.getWindowOffsetMs()};
    } else if (tumbleHint.getType() == TumbleAggregationHint.Type.INSTANT) {
      windowDef = new long[]{1};
      Preconditions.checkArgument(tumbleHint.getInputTimestampIdx()==windowTimestampIdx);
    } else {
      throw new UnsupportedOperationException(
          "Invalid tumble window type: " + tumbleHint.getType());
    }
    makeWindow(relBuilder, windowFunction, tumbleHint.getInputTimestampIdx(), windowDef);

  }

  private void handleSlidingWindow(int timestampIdx, SlidingAggregationHint slideHint,
      RelNode input, RelBuilder relBuilder) {
    relBuilder.push(input);
    var intervalsMs = new long[]{slideHint.getSlideWidthMs(), slideHint.getIntervalWidthMs()};
    SqlOperator windowFunction = FlinkSqlOperatorTable.HOP;
    makeWindow(relBuilder, windowFunction, timestampIdx, intervalsMs);
  }

  private void handleRegularAggregation(RelBuilder relBuilder,
      ImmutableBitSet groupBy, List<AggregateCall> aggCalls, RelNode input) {
    relBuilder.push(input);
    relBuilder.aggregate(relBuilder.groupKey(groupBy), aggCalls);
  }

  private RelBuilder makeWindow(RelBuilder relBuilder, SqlOperator operator,
      int timestampIdx, long[] intervalsMs) {
    Preconditions.checkArgument(intervalsMs != null && intervalsMs.length > 0);
    var rexBuilder = getRexBuilder(relBuilder);
    var input = relBuilder.peek();

    var operandList = createOperandList(rexBuilder, input, timestampIdx, intervalsMs);

    //this window functions adds 3 columns to end of relation: window_start/_end/_time
    //TODO: This should actually be a COLLECTION_TABLE + a call to tumble function
    relBuilder.functionScan(FlinkSqlOperatorTable.TUMBLE, 1, operandList);
    var tfs = (LogicalTableFunctionScan) relBuilder.build();

    //Flink expects an inputref for the last column of the original relation as the first operand
    operandList = ListUtils.union(
        List.of(rexBuilder.makeInputRef(input, input.getRowType().getFieldCount() - 1)),
        operandList.subList(1, operandList.size()));
    relBuilder.push(tfs.copy(tfs.getTraitSet(), tfs.getInputs(),
        rexBuilder.makeCall(tfs.getRowType(), operator, operandList),
        tfs.getElementType(), tfs.getRowType(), tfs.getColumnMappings()));
    return relBuilder;
  }

  private List<RexNode> createOperandList(RexBuilder rexBuilder, RelNode input,
      int timestampIdx, long[] intervalsMs) {
    List<RexNode> operandList = new ArrayList<>();
    operandList.add(rexBuilder.makeRangeReference(input));
    operandList.add(rexBuilder.makeCall(FlinkSqlOperatorTable.DESCRIPTOR,
        rexBuilder.makeInputRef(input, timestampIdx)));
    for (long intervalArg : intervalsMs) {
      operandList.add(CalciteUtil.makeTimeInterval(intervalArg, rexBuilder));
    }
    return operandList;
  }


  /** Rule configuration. */
  @Value.Immutable
  public interface ExpandWindowHintRuleConfig extends RelRule.Config {
    ExpandWindowHintRule.Config DEFAULT = ImmutableExpandWindowHintRuleConfig.builder()
        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .description("ExpandWindowHintRule")
        .operandSupplier(b0 ->
            b0.operand(LogicalAggregate.class).oneInput(
                b1 -> b1.operand(RelNode.class).anyInputs()))
        .build();

    @Override default ExpandWindowHintRule toRule() {
      return new ExpandWindowHintRule(this);
    }
  }
}
