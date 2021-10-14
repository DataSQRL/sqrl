package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.physical.VariableAllocator;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.TableSubquery;
import com.google.common.collect.Iterables;
import java.util.List;

public class SqlQueryRewriter extends AstVisitor<Object, Object> {

  private final QueryRelationDefinition relation;
  private final StatementAnalysis statementAnalysis;
  private final VariableAllocator allocator;

  public SqlQueryRewriter(QueryRelationDefinition relation, VariableAllocator allocator) {
    this.relation = relation;
    this.statementAnalysis = relation.getStatementAnalysis();
    this.allocator = allocator;
  }

  @Override
  protected Object visitQuery(Query node, Object context) {
    QueryBuilder builder = (QueryBuilder)
        node.getQueryBody().accept(this, context);


    return super.visitQuery(node, context);
  }

  @Override
  protected Object visitQuerySpecification(QuerySpecification node, Object context) {


    return super.visitQuerySpecification(node, context);
  }

  @Override
  protected Object visitTableSubquery(TableSubquery node, Object context) {
    return super.visitTableSubquery(node, context);
  }

  @Override
  protected Object visitTable(Table node, Object context) {
    return super.visitTable(node, context);
  }

  class QueryBuilder {

  }
}

