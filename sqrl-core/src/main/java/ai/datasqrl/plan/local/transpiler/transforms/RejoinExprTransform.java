package ai.datasqrl.plan.local.transpiler.transforms;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.plan.local.transpiler.NormalizerUtils;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableRef;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.attributes.ForeignKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RejoinExprTransform {

  /**
   * Join expression back on to the host query.
   */
  public static RelationNorm transform(QuerySpecNorm node, Table table) {
    TableNodeNorm tableNodeNorm = new TableNodeNorm(Optional.empty(), table.getPath(), Optional.empty(), new TableRef(table),
        false, List.of());
    List<Expression> columns = table.getColumns().stream()
        .filter(field -> !field.containsAttribute(ForeignKey.class))
        .map(c-> ResolvedColumn.of(tableNodeNorm, c))
        .collect(Collectors.toList());

    columns.add(new ReferenceExpression(node, node.getSelect().getSelectItems().get(0).getExpression()));

    JoinNorm joinNorm = new JoinNorm(Optional.empty(), node.isAggregating() ? Type.LEFT : Type.INNER,
        tableNodeNorm,
        node,
        Optional.of(new JoinOn(Optional.empty(), NormalizerUtils.primaryKeyCriteria(table, node, tableNodeNorm))));

    List<ResolvedColumn> ppk = table.getColumns().stream()
        .filter(field -> field.containsAttribute(ForeignKey.class))
        .map(c-> ResolvedColumn.of(tableNodeNorm, c))
        .collect(Collectors.toList());

    List<SingleColumn> columnList = columns.stream()
        .map(c->new SingleColumn(Optional.empty(), c, Optional.empty()))
        .collect(Collectors.toList());

    QuerySpecNorm outer = new QuerySpecNorm(Optional.empty(),
        ppk,
        List.of(),
        new SelectNorm(Optional.empty(), false, columnList),
        joinNorm,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of()
    );
    return outer;
  }
}
