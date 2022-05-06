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
//    //3. check for row_num in subquery

//    //4. Walk join tree
//
//    throw new RuntimeException("");
    return List.of(selectList.getSelectItems().get(0).getExpression());
  }

}
