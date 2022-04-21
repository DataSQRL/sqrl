package ai.datasqrl.transform;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryBody;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.validate.scopes.StatementScope;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryTransformer extends AstVisitor<Node, StatementScope> {

  public QueryTransformer() {
  }

  @Override
  public Node visitNode(Node node, StatementScope context) {
    throw new RuntimeException(
        String.format("Could not process node %s : %s", node.getClass().getName(), node));
  }

  @Override
  public Node visitQuery(Query node, StatementScope scope) {
    Relation relation = node.getQueryBody().accept(new RelationTransformer(), scope);
    Query query = new Query(node.getLocation(), (QueryBody) relation,
        node.getOrderBy(), node.getLimit());
    log.info("Generated Query: {}", NodeFormatter.accept(query));

    //TODO: order & limit for Set operations
    return query;
  }
}
