package ai.datasqrl.plan.local.transpiler.util;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionTreeRewriter;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.SimpleGroupBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RelationNormRewriter extends AstVisitor<RelationNorm, Void> {
  ExpressionNormRewriter rewriter;
  Map<RelationNorm, RelationNorm> normMapping;

  public RelationNormRewriter() {
    this.normMapping = new HashMap<>();
    this.rewriter = new ExpressionNormRewriter(normMapping);
  }
  @Override
  public RelationNorm visitQuerySpecNorm(QuerySpecNorm node, Void context) {
    QuerySpecNorm querySpecNorm = new QuerySpecNorm(node.getLocation(),
        rewriteList(node.getParentPrimaryKeys()),
        rewriteList(node.getAddedPrimaryKeys()),
        rewriteSelect(node.getSelect()),
        node.getFrom().accept(this, null),
        node.getWhere().map(w->ExpressionTreeRewriter.rewriteWith(rewriter, w)),
        node.getGroupBy().map(g->new GroupBy(g.getLocation(), new SimpleGroupBy(rewriteList(g.getGroupingElement().getExpressions())))),
        node.getHaving().map(w->ExpressionTreeRewriter.rewriteWith(rewriter, w)),
        node.getOrderBy().map(ord-> new OrderBy(ord.getLocation(), ord.getSortItems().stream()
            .map(o->new SortItem(o.getLocation(), ExpressionTreeRewriter.rewriteWith(rewriter, o.getSortKey()), o.getOrdering()))
            .collect(Collectors.toList()))),
        node.getLimit(),
        rewriteList(node.getPrimaryKeys())
    );
    normMapping.put(node, querySpecNorm);
    return querySpecNorm;
  }

  private SelectNorm rewriteSelect(SelectNorm select) {
    return new SelectNorm(select.getLocation(), select.isDistinct(), select.getSelectItems().stream()
        .map(s->new SingleColumn(s.getLocation(), ExpressionTreeRewriter.rewriteWith(rewriter, s.getExpression()), s.getAlias()))
        .collect(Collectors.toList()));
  }

  private <T extends RelationNorm> List<T> rewriteNodeList(List<T> node) {
    return (List<T>)node.stream()
        .map(e->e.accept(this, null))
        .collect(Collectors.toList());
  }

  private <T extends Expression> List<T> rewriteList(List<T> node) {
    return (List<T>)node.stream()
        .map(e->ExpressionTreeRewriter.rewriteWith(rewriter, e))
        .collect(Collectors.toList());
  }
  private <T extends Expression> Set<T> rewriteSet(Set<T> node) {
    return (Set<T>)node.stream()
        .map(e->ExpressionTreeRewriter.rewriteWith(rewriter, e))
        .collect(Collectors.toSet());
  }

  @Override
  public RelationNorm visitTableNorm(TableNodeNorm node, Void context) {
    TableNodeNorm tableNodeNorm = new TableNodeNorm(node.getLocation(), node.getName(), node.getAlias(), node.getRef(),
        false, List.of());
    normMapping.put(node, tableNodeNorm);
    return tableNodeNorm;
  }

  @Override
  public RelationNorm visitJoinNorm(JoinNorm node, Void context) {
    JoinNorm joinNorm = new JoinNorm(node.getLocation(),
        node.getType(),
        node.getLeft().accept(this, null),
        node.getRight().accept(this, null),
        node.getCriteria().map(c->new JoinOn(c.getLocation(),
            ExpressionTreeRewriter.rewriteWith(rewriter, ((JoinOn)c).getExpression())))
    );
    normMapping.put(node, joinNorm);
    return joinNorm;
  }
}
