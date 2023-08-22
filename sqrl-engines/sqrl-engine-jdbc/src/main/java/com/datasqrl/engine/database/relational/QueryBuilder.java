/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.CalciteUtil.InjectParentRelNode;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class QueryBuilder {

  private RexBuilder rexBuilder;

  public Map<IdentifiedQuery, QueryTemplate> planQueries(
      List<ReadQuery> databaseQueries) {
    Map<IdentifiedQuery, QueryTemplate> resultQueries = new HashMap<>();
    for (PhysicalDAGPlan.ReadQuery query : databaseQueries) {
      resultQueries.put(query.getQuery(), planQuery(query));
    }
    return resultQueries;
  }

  private QueryTemplate planQuery(PhysicalDAGPlan.ReadQuery query) {
    RelNode relNode = query.getRelNode();
    relNode = CalciteUtil.applyRexShuttleRecursively(relNode, new FunctionNameRewriter());
    return new QueryTemplate(relNode);
  }

  private class FunctionNameRewriter extends RexShuttle implements InjectParentRelNode {

    @Override
    public RexNode visitCall(RexCall call) {
      boolean[] update = new boolean[]{false};
      List<RexNode> clonedOperands = this.visitList(call.operands, update);
      SqlOperator operator = call.getOperator();
      RelDataType datatype = call.getType();
      String operatorName = operator.getName();
      if (operatorName.equalsIgnoreCase("NOW")) {
        update[0] = true;
        Preconditions.checkArgument(clonedOperands.isEmpty());
        int precision = datatype.getPrecision();
        operator = SqlStdOperatorTable.CURRENT_TIMESTAMP;
      } else if (call.isA(SqlKind.BINARY_COMPARISON)) {
        Optional<RexNode> greaterZero = CalciteUtil.isGreaterZero(call);
        if (greaterZero.isPresent() && greaterZero.get() instanceof RexCall) {
          RexCall distanceCall = (RexCall) greaterZero.get();
          if (distanceCall.getOperator().isName("textsearch", false)) {
            //TODO: if RexNode is a call to textSearch, replace with predicate for a special postgres function
            update[0] = true;
            operator = SqlStdOperatorTable.NOT_EQUALS;
            clonedOperands = List.of(distanceCall.operands.get(0),rexBuilder.makeLiteral(""));
          }
        }

      }

      return update[0] ? rexBuilder.makeCall(datatype, operator, clonedOperands) : call;
    }

    private boolean parentRelnodeIsFilter = false;

    @Override
    public void setParentRelNode(RelNode relNode) {
      this.parentRelnodeIsFilter = (relNode instanceof LogicalFilter);
    }
  }

}
