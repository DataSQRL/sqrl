package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.TableSubquery;

public class SqlQueryRewriter extends AstVisitor<Scope, Scope> {

  public Scope rewrite(Query node, Scope scope) {
    Scope s = node.getQueryBody().accept(this, scope);

    return null;
  }

  @Override
  protected Scope visitQuerySpecification(QuerySpecification node, Scope context) {
    node.getFrom();
    return super.visitQuerySpecification(node, context);
  }

  @Override
  protected Scope visitTableSubquery(TableSubquery node, Scope context) {
    return super.visitTableSubquery(node, context);
  }

  @Override
  protected Scope visitTable(Table node, Scope context) {
    return super.visitTable(node, context);
  }
}

