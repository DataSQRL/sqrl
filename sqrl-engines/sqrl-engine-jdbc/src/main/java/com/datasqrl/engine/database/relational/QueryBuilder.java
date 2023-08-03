/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
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

  private class FunctionNameRewriter extends RexShuttle {

    @Override
    public RexNode visitCall(RexCall call) {
      boolean[] update = new boolean[]{false};
      List<RexNode> clonedOperands = this.visitList(call.operands, update);
      SqlOperator operator = call.getOperator();
      RelDataType datatype = call.getType();
      if (operator.getName().equalsIgnoreCase("NOW")) {
        update[0] = true;
        Preconditions.checkArgument(clonedOperands.isEmpty());
        int precision = datatype.getPrecision();
        operator = SqlStdOperatorTable.CURRENT_TIMESTAMP;
      }

      return update[0] ? rexBuilder.makeCall(datatype, operator, clonedOperands) : call;
    }

  }

}
