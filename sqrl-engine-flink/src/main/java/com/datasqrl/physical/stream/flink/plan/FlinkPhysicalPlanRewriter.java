package com.datasqrl.physical.stream.flink.plan;


import com.datasqrl.plan.calcite.hints.*;
import com.datasqrl.plan.calcite.table.SourceRelationalTable;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Rewrites the streaming physical plan generated by the optimizer for Flink.
 * Specifically, this means:
 *  - Injecting a Flink Calcite cluster to replace the SQRL calcite cluster
 *  - Adding watermarks and propagating timestamp columns
 *  - Expanding temporal joins
 *  - Expanding time-based aggregations into Flink window aggregations
 *  - Handling interval joins
 *
 *  This class is not thread safe. You should construct a new instance for each rewriting or
 *  simply use {@link #rewrite(StreamTableEnvironmentImpl, RelNode)}.
 */
public class FlinkPhysicalPlanRewriter extends RelShuttleImpl {
  StreamTableEnvironmentImpl tEnv;
  private boolean isTop = true;

  public FlinkPhysicalPlanRewriter(StreamTableEnvironmentImpl tEnv) {
    this.tEnv = tEnv;
  }

  public static RelNode rewrite(StreamTableEnvironmentImpl tEnv, RelNode input) {
    return input.accept(new FlinkPhysicalPlanRewriter(tEnv));
  }

  private FlinkRelBuilder getBuilder() {
    return ((StreamPlanner) tEnv.getPlanner()).getRelBuilder();
  }

  @Override
  public RelNode visit(TableScan scan) {
    SourceRelationalTable t = scan.getTable().unwrap(SourceRelationalTable.class);
    String tableName = t.getNameId();
    FlinkRelBuilder relBuilder = getBuilder();
    relBuilder.scan(tableName);
    SqrlHint.fromRel(scan, WatermarkHint.CONSTRUCTOR).ifPresent(watermark -> addWatermark(relBuilder,watermark.getTimestampIdx()));
    return relBuilder.build();
  }

  private boolean isTop() {
    boolean isTopTmp = isTop;
    isTop = false;
    return isTopTmp;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    boolean forceProject = isTop(); //We want to make sure we preserve the projection at the top so the names line up
    FlinkRelBuilder relBuilder = getBuilder();
    relBuilder.push(project.getInput().accept(this));
    List<String> projectNames = project.getRowType().getFieldNames();
    relBuilder.project(project.getProjects().stream().map(rex -> rewrite(rex, relBuilder)).collect(Collectors.toList()),
              projectNames, forceProject);
    SqrlHint.fromRel(project, WatermarkHint.CONSTRUCTOR).ifPresent(watermark -> addWatermark(relBuilder,watermark.getTimestampIdx()));
    return relBuilder.build();
  }

  private void addWatermark(FlinkRelBuilder relBuilder, int timestampIndex) {
    relBuilder.watermark(timestampIndex,getRexBuilder(relBuilder).makeInputRef(relBuilder.peek(), timestampIndex));
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    FlinkRelBuilder relBuilder = getBuilder();
    relBuilder.push(filter.getInput().accept(this));
    relBuilder.filter(filter.getVariablesSet(),rewrite(filter.getCondition(),relBuilder));
    return relBuilder.build();
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    FlinkRelBuilder relBuilder = getBuilder();
    RelNode left = join.getLeft().accept(this), right = join.getRight().accept(this);
    Optional<TemporalJoinHint> temporalHintOpt = SqrlHint.fromRel(join,TemporalJoinHint.CONSTRUCTOR);
    if (temporalHintOpt.isPresent()) {
      TemporalJoinHint temporalHint = temporalHintOpt.get();
      FlinkRexBuilder rexBuilder = FlinkPhysicalPlanRewriter.getRexBuilder(relBuilder);
      Holder<RexCorrelVariable> correlVar = Holder.of(null);
      relBuilder.push(left).variable(correlVar);
      relBuilder.push(right);
      if (!SqrlRexUtil.hasDeduplicationWindow(right)) {
        addDeduplicationWindow(relBuilder,temporalHint);
      }
      //CalciteUtil.addIdentityProjection(relBuilder, relBuilder.peek().getRowType().getFieldCount());
      relBuilder.snapshot(rexBuilder.makeFieldAccess(correlVar.get(),temporalHint.getStreamTimestampIdx()));
      CorrelateRexRewriter correlRewriter = new CorrelateRexRewriter(rexBuilder,left.getRowType().getFieldList(),
              correlVar, right.getRowType().getFieldList());
      relBuilder.filter(correlRewriter.rewrite(join.getCondition()));
      Set<Integer> usedLeftFieldIdx = correlRewriter.usedLeftFieldIdx;
      usedLeftFieldIdx.add(temporalHint.getStreamTimestampIdx());
      relBuilder.correlate(join.getJoinType(), correlVar.get().id, usedLeftFieldIdx.stream()
              .map(idx -> rexBuilder.makeInputRef(left,idx)).collect(Collectors.toList()));
      return relBuilder.build();
    } else {
      relBuilder.push(left);
      relBuilder.push(right);
      JoinRelType joinType = join.getJoinType();
      relBuilder.join(joinType, rewrite(join.getCondition(), relBuilder, left, right));
      return relBuilder.build();
    }
  }

  private void addDeduplicationWindow(FlinkRelBuilder relBuilder, TemporalJoinHint temporalHint) {
    final RelDataType inputType = relBuilder.peek().getRowType();
    List<RexNode> partitionKeys = Arrays.stream(temporalHint.getStatePrimaryKeys())
            .mapToObj(idx -> RexInputRef.of(idx,inputType)).collect(Collectors.toList());
    List<RexFieldCollation> fieldCollations = List.of(new RexFieldCollation(RexInputRef.of(temporalHint.getStateTimestampIdx(), inputType),
            Set.of(SqlKind.DESCENDING, SqlKind.NULLS_LAST)));
    List<RexNode> projects = inputType.getFieldList().stream().map(f -> RexInputRef.of(f.getIndex(),inputType)).collect(Collectors.toList());
    int rowNumberIdx = projects.size();
    projects = new ArrayList(projects);
    SqrlRexUtil rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
    //Add row_number (since it always applies)
    projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER,partitionKeys,fieldCollations));
    relBuilder.project(projects);
    RelDataType windowType = relBuilder.peek().getRowType();
    relBuilder.filter(SqrlRexUtil.makeWindowLimitFilter(getRexBuilder(relBuilder), 1, rowNumberIdx, windowType));
    CalciteUtil.addIdentityProjection(relBuilder,rowNumberIdx); //project row_number back out
    throw new UnsupportedOperationException("Not yet implemented, see issue#67");
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    FlinkRelBuilder relBuilder = getBuilder();
    Optional<TumbleAggregationHint> tumbleHintOpt = SqrlHint.fromRel(aggregate, TumbleAggregationHint.CONSTRUCTOR);
    Optional<SlidingAggregationHint> slideHintOpt = SqrlHint.fromRel(aggregate, SlidingAggregationHint.CONSTRUCTOR);
    ImmutableBitSet groupBy = Iterables.getOnlyElement(aggregate.groupSets);
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    RelNode input = aggregate.getInput().accept(this);
    if (tumbleHintOpt.isPresent() || slideHintOpt.isPresent()) {
      Preconditions.checkArgument(tumbleHintOpt.isPresent() ^ slideHintOpt.isPresent());
      FlinkRexBuilder rexBuilder = getRexBuilder(relBuilder);
      int inputFieldCount = input.getRowType().getFieldCount();
      RelDataType inputType = input.getRowType();

      final int timestampIdx;
      final long[] intervalsMs;
      final SqlOperator windowFunction;

      if (tumbleHintOpt.isPresent()) {
        TumbleAggregationHint tumbleHint = tumbleHintOpt.get();
        timestampIdx = tumbleHint.getTimestampIdx();
        windowFunction = FlinkSqlOperatorTable.TUMBLE;
        if (tumbleHint.getType()== TumbleAggregationHint.Type.FUNCTION) {
          //Extract bucketing function from project
          Preconditions.checkArgument(input instanceof LogicalProject, "Expected projection as input");
          relBuilder.push(input.getInput(0));
          List<RexNode> projects = new ArrayList<>(((LogicalProject) input).getProjects());
          projects.set(timestampIdx, rexBuilder.makeInputRef(input.getInput(0), tumbleHint.getInputTimestampIdx()));
          long intervalMs = tumbleHint.getIntervalMS();
          intervalsMs = new long[]{intervalMs};

          relBuilder.project(projects, inputType.getFieldNames());
        } else if (tumbleHint.getType()== TumbleAggregationHint.Type.INSTANT) {
          relBuilder.push(input);
          intervalsMs = new long[]{1};
        } else {
          throw new UnsupportedOperationException("Invalid tumble window type: " + tumbleHint.getType());
        }
      } else {
        relBuilder.push(input);
        SlidingAggregationHint slideHint = slideHintOpt.get();
        timestampIdx = slideHint.getTimestampIdx();
        intervalsMs = new long[]{slideHint.getSlideWidthMs(), slideHint.getIntervalWidthMs()};
        windowFunction = FlinkSqlOperatorTable.HOP;
      }

      makeWindow(relBuilder,windowFunction,timestampIdx,intervalsMs);
      //Need to add all 3 window columns that are added to groupBy and then project out all but window_time
      int window_start = inputFieldCount, window_end = inputFieldCount+1, window_time = inputFieldCount+2;
      List<Integer> groupByIdx = new ArrayList<>();
      List<Integer> projectIdx = new ArrayList<>();
      List<String> projectNames = new ArrayList<>();
      int index = 0;
      int window_time_idx = (groupBy.cardinality()-1)+3-1; //Points at window_time at the end of groupByIdx
      for (int idx : groupBy.asList()) {
        if (idx==timestampIdx) {
          projectIdx.add(window_time_idx);
          projectNames.add(inputType.getFieldNames().get(timestampIdx));
        } else {
          groupByIdx.add(idx);
          projectIdx.add(index++);
          projectNames.add(inputType.getFieldNames().get(idx));
        }
      }
      groupByIdx.add(window_start); groupByIdx.add(window_end);
      groupByIdx.add(window_time); //Window_time is new timestamp
      index+=3;
      assert window_time_idx==index-1;

      for (int i = 0; i < aggCalls.size(); i++) {
        projectIdx.add(index++);
        projectNames.add(null);
      }
      relBuilder.aggregate(relBuilder.groupKey(Ints.toArray(groupByIdx)),aggCalls);
      relBuilder.project(projectIdx.stream().map(idx -> rexBuilder.makeInputRef(relBuilder.peek(), idx))
              .collect(Collectors.toList()),projectNames);
    } else {
      //Normal aggregation
      relBuilder.push(input);
      relBuilder.aggregate(relBuilder.groupKey(groupBy),aggCalls);
    }
    return relBuilder.build();
  }

  private FlinkRelBuilder makeWindow(FlinkRelBuilder relBuilder, SqlOperator operator, int timestampIdx, long... intervalsMs) {
    Preconditions.checkArgument(intervalsMs!=null && intervalsMs.length>0);
    FlinkRexBuilder rexBuilder = getRexBuilder(relBuilder);
    RelNode input = relBuilder.peek();

    List<RexNode> operandList = new ArrayList<>();
    operandList.add(rexBuilder.makeRangeReference(input));
    operandList.add(rexBuilder.makeCall(FlinkSqlOperatorTable.DESCRIPTOR,rexBuilder.makeInputRef(input,timestampIdx)));
    for (long intervalArg : intervalsMs) {
      operandList.add(CalciteUtil.makeTimeInterval(intervalArg, rexBuilder));
    }

    //this window functions adds 3 columns to end of relation: window_start/_end/_time
    relBuilder.functionScan(FlinkSqlOperatorTable.TUMBLE,1,operandList);
    LogicalTableFunctionScan tfs = (LogicalTableFunctionScan) relBuilder.build();

    //Flink expects an inputref for the last column of the original relation as the first operand
    operandList = ListUtils.union(List.of(rexBuilder.makeInputRef(input,input.getRowType().getFieldCount()-1)),
            operandList.subList(1,operandList.size()));
    relBuilder.push(tfs.copy(tfs.getTraitSet(),tfs.getInputs(),
            rexBuilder.makeCall(tfs.getRowType(), operator, operandList),
            tfs.getElementType(),tfs.getRowType(),tfs.getColumnMappings()));
    return relBuilder;
  }

  @Override
  public RelNode visit(LogicalValues values) {
    return getBuilder().values(values.tuples, values.getRowType()).build();
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    Preconditions.checkArgument(union.all);
    FlinkRelBuilder relBuilder = getBuilder();
    int numInputs = 0;
    for (RelNode input : union.getInputs()) {
      relBuilder.push(input.accept(this));
      numInputs++;
    }
    relBuilder.union(union.all, numInputs);
    return relBuilder.build();
  }

  private Pair<CorrelationId,CorrelationId> replaceCorrelId = null;

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    FlinkRelBuilder relBuilder = getBuilder();
    relBuilder.push(correlate.getLeft().accept(this));
    RelDataType base = relBuilder.peek().getRowType();
    CorrelationId newid = relBuilder.getCluster().createCorrel();
    replaceCorrelId = Pair.of(correlate.getCorrelationId(),newid);
    relBuilder.push(correlate.getRight().accept(this));
    replaceCorrelId = null;
    relBuilder.correlate(correlate.getJoinType(), newid,
            correlate.getRequiredColumns().asList().stream().map(i -> relBuilder.getRexBuilder().makeInputRef(base,i)).collect(Collectors.toList()));
    return relBuilder.build();
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof Uncollect) {
      Uncollect uncollect = (Uncollect) other;
      RelBuilder relBuilder = getBuilder();
      relBuilder.push(uncollect.getInput().accept(this));
      relBuilder.uncollect(List.of(),uncollect.withOrdinality);
      return relBuilder.build();
    }
    throw new UnsupportedOperationException("not yet implemented:" + other.getClass());
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    throw new UnsupportedOperationException("Not yet supported");
//    List<RelNode> inputs = scan.getInputs().stream()
//        .map(this::visit)
//        .collect(Collectors.toList());
//    return new LogicalTableFunctionScan(cluster, defaultTrait, inputs, scan.getCall(),
//        scan.getElementType(), scan.getRowType(), scan.getColumnMappings());
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    throw new UnsupportedOperationException("Sorts are not supported during materialization");
  }


  /*
  ====== Rewriting RexNodes
   */

  private static FlinkRexBuilder getRexBuilder(FlinkRelBuilder relBuilder) {
    return new FlinkRexBuilder(relBuilder.getTypeFactory());
  }

  private RexNode rewrite(RexNode node, FlinkRelBuilder relBuilder) {
    return rewrite(node, relBuilder, relBuilder.peek());
  }

  private RexNode rewrite(RexNode node, FlinkRelBuilder relBuilder, RelNode... inputNodes) {
    Preconditions.checkArgument(inputNodes!=null && inputNodes.length>0);
    List<RelDataTypeField> fields;
    if (inputNodes.length==1) {
      fields = inputNodes[0].getRowType().getFieldList();
    } else {
      fields = new ArrayList<>();
      for (RelNode input : inputNodes) {
        fields.addAll(input.getRowType().getFieldList());
      }
    }
    return node.accept(new RexRewriter(fields,
            FlinkPhysicalPlanRewriter.getRexBuilder(relBuilder)));
  }

  @AllArgsConstructor
  private class RexRewriter extends RexShuttle {

    final List<RelDataTypeField> inputFields;
    final FlinkRexBuilder rexBuilder;

    @Override
    public RexNode visitInputRef(RexInputRef ref) {
      return rexBuilder.makeInputRef(inputFields.get(ref.getIndex()).getType(),ref.getIndex());
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      if (replaceCorrelId!=null && variable.id.equals(replaceCorrelId.getKey())) {
        return rexBuilder.makeCorrel(variable.getType(), replaceCorrelId.getValue());
      } else {
        return variable;
      }
    }

    @Override
    public RexNode visitCall(RexCall call) {
      boolean[] update = new boolean[]{false};
      List<RexNode> clonedOperands = this.visitList(call.operands, update);
      SqlOperator operator = call.getOperator();
      RelDataType datatype = call.getType();
      if (operator.equals(SqlStdOperatorTable.CURRENT_TIMESTAMP)) {
        update[0] = true;
        operator = FlinkSqlOperatorTable.PROCTIME;
        //TODO: remove this condition for now from the pipeline since Flink cannot handle the interval
        return rexBuilder.makeZeroLiteral(call.getType());
      }
      return update[0] ? rexBuilder.makeCall(datatype,operator,clonedOperands) : call;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      if (literal.getTypeName()== SqlTypeName.INTERVAL_SECOND) {
        BigDecimal intervalMs = literal.getValueAs(BigDecimal.class);
        //This does not seem to work in Flink
        SqlIntervalQualifier sqlIntervalQualifier =
                new SqlIntervalQualifier(TimeUnit.YEAR, 3, TimeUnit.YEAR, 3, SqlParserPos.ZERO);
        return rexBuilder.makeIntervalLiteral(intervalMs, sqlIntervalQualifier);
      }
      return literal;
    }
  }

  private class CorrelateRexRewriter extends RexRewriter {

    final List<RelDataTypeField> leftFields;
    final Holder<RexCorrelVariable> leftCorrelVar;
    final List<RelDataTypeField> rightFields;
    final Set<Integer> usedLeftFieldIdx = new HashSet<>();

    public CorrelateRexRewriter(FlinkRexBuilder rexBuilder,
                                List<RelDataTypeField> leftFields, Holder<RexCorrelVariable> leftCorrelVar,
                                List<RelDataTypeField> rightFields) {
      super(List.of(), rexBuilder);
      this.leftFields = leftFields;
      this.leftCorrelVar = leftCorrelVar;
      this.rightFields = rightFields;
    }

    @Override
    public RexNode visitInputRef(RexInputRef ref) {
      int idx = ref.getIndex();
      if (idx<leftFields.size()) {
        usedLeftFieldIdx.add(idx);
        return rexBuilder.makeFieldAccess(leftCorrelVar.get(), idx);

      } else {
        idx = idx - leftFields.size();
        return rexBuilder.makeInputRef(rightFields.get(idx).getType(),idx);
      }
    }

    public RexNode rewrite(RexNode node) {
      return node.accept(this);
    }

  }

}
