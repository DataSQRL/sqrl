package ai.datasqrl.plan.local.transpiler;

import ai.datasqrl.function.SqlNativeFunction;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class PrimaryKeyDeriver {

  private final RelationScope scope;
  private final SelectNorm selectList;
  private final Set<ReferenceOrdinal> group;
  private final RelationNorm fromNorm;

  public PrimaryKeyDeriver(RelationScope scope, SelectNorm selectList,
      Set<ReferenceOrdinal> group, RelationNorm fromNorm) {
    this.scope = scope;
    this.selectList = selectList;
    this.group = group;
    this.fromNorm = fromNorm;
  }

  public List<Expression> get() {
    //1. Check if there is a distinct
    if (selectList.isDistinct()) {
      return selectList.getAsExpressions();
    }
    //2. check group by
    if (!group.isEmpty()) {
      return group.stream()
          .map(g->selectList.getSelectItems().get(g.getOrdinal()).getExpression())
          .collect(Collectors.toList());
    }
//    //3. check for row_num in subquery
//    SingleColumn col;
//    if (fromNorm instanceof QuerySpecNorm && (col = hasRowNum((QuerySpecNorm) fromNorm)) != null) {
//      ResolvedFunctionCall functionCall = (ResolvedFunctionCall)col.getExpression();
//      Window window = functionCall.getOldExpression().getOver().get();
//      return window.getPartitionBy();
//    }
//    //4. Walk join tree
//
//    throw new RuntimeException("");
    return List.of(selectList.getSelectItems().get(0).getExpression());
  }

  private SingleColumn hasRowNum(QuerySpecNorm fromNorm) {
    for (SingleColumn col : fromNorm.getSelect().getSelectItems()) {
      if (col.getExpression() instanceof ResolvedFunctionCall &&
          ((ResolvedFunctionCall)col.getExpression()).getFunction() instanceof SqlNativeFunction &&
          ((SqlNativeFunction)((ResolvedFunctionCall)col.getExpression()).getFunction()).getOp() == SqlStdOperatorTable.ROW_NUMBER)
          {
        return col;
      }
    }
    return null;
  }
}
