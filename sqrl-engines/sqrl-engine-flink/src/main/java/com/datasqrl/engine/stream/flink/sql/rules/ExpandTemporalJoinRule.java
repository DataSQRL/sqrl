package com.datasqrl.engine.stream.flink.sql.rules;

import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.TemporalJoinHint;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

public class ExpandTemporalJoinRule extends RelOptRule {

  public ExpandTemporalJoinRule() {
    super(operand(LogicalJoin.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    LogicalJoin join = relOptRuleCall.rel(0);

    Optional<TemporalJoinHint> temporalHintOpt = SqrlHint.fromRel(join,
        TemporalJoinHint.CONSTRUCTOR);

    temporalHintOpt
        .map(hint->handleTemporalJoin(relOptRuleCall, join, hint))
        .ifPresent(relOptRuleCall::transformTo);
  }


  private RelNode handleTemporalJoin(RelOptRuleCall relOptRuleCall, LogicalJoin join,
      TemporalJoinHint temporalHint) {
    RelBuilder relBuilder = relOptRuleCall.builder();
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    Holder<RexCorrelVariable> correlVar = Holder.of(null);
    relBuilder.push(join.getLeft()).variable(correlVar);
    relBuilder.push(join.getRight());
    relBuilder.snapshot(
        rexBuilder.makeFieldAccess(correlVar.get(), temporalHint.getStreamTimestampIdx()));
    CorrelateRexRewriter correlRewriter = new CorrelateRexRewriter(rexBuilder,
        join.getLeft().getRowType().getFieldList(),
        correlVar, join.getRight().getRowType().getFieldList());
    relBuilder.filter(correlRewriter.rewrite(join.getCondition()));
    Set<Integer> usedLeftFieldIdx = correlRewriter.usedLeftFieldIdx;
    usedLeftFieldIdx.add(temporalHint.getStreamTimestampIdx());
    relBuilder.correlate(join.getJoinType(), correlVar.get().id, usedLeftFieldIdx.stream()
        .map(idx -> rexBuilder.makeInputRef(join.getLeft(), idx)).collect(Collectors.toList()));
    return relBuilder.build();
  }


  private class CorrelateRexRewriter extends RexRewriter {

    final List<RelDataTypeField> leftFields;
    final Holder<RexCorrelVariable> leftCorrelVar;
    final List<RelDataTypeField> rightFields;
    final Set<Integer> usedLeftFieldIdx = new HashSet<>();

    public CorrelateRexRewriter(RexBuilder rexBuilder,
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
      if (idx < leftFields.size()) {
        usedLeftFieldIdx.add(idx);
        return rexBuilder.makeFieldAccess(leftCorrelVar.get(), idx);

      } else {
        idx = idx - leftFields.size();
        return rexBuilder.makeInputRef(rightFields.get(idx).getType(), idx);
      }
    }

    public RexNode rewrite(RexNode node) {
      return node.accept(this);
    }

  }
  @AllArgsConstructor
  private class RexRewriter extends RexShuttle {

    final List<RelDataTypeField> inputFields;
    final RexBuilder rexBuilder;

    @Override
    public RexNode visitInputRef(RexInputRef ref) {
      return rexBuilder.makeInputRef(inputFields.get(ref.getIndex()).getType(), ref.getIndex());
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
      return update[0] ? rexBuilder.makeCall(datatype, operator, clonedOperands) : call;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      if (literal.getTypeName() == SqlTypeName.INTERVAL_SECOND) {
        BigDecimal intervalMs = literal.getValueAs(BigDecimal.class);
        //This does not seem to work in Flink
        SqlIntervalQualifier sqlIntervalQualifier =
            new SqlIntervalQualifier(TimeUnit.YEAR, 3, TimeUnit.YEAR, 3, SqlParserPos.ZERO);
        return rexBuilder.makeIntervalLiteral(intervalMs, sqlIntervalQualifier);
      }
      return literal;
    }
  }
}
