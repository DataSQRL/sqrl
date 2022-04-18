package ai.datasqrl.transform;

import static ai.datasqrl.parse.util.SqrlNodeUtil.mapToOrdinal;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryBody;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.validate.scopes.StatementScope;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryTransformer extends AstVisitor<Scope, StatementScope> {
//
//  private final AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());
//
//  private List<JoinResult> additionalJoins = new ArrayList<>();
//
//  public static final AliasGenerator gen = new AliasGenerator();

  public QueryTransformer() {
  }

  @Override
  public Scope visitNode(Node node, StatementScope context) {
    throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
  }

  @Override
  public Scope visitQuery(Query node, StatementScope scope) {
    Relation relation = node.getQueryBody().accept(new RelationTransformer(), scope);
    Query query = new Query(node.getLocation(), (QueryBody) relation,
        node.getOrderBy(), node.getLimit());
    log.info("Generated Query: {}", NodeFormatter.accept(query));

    //TODO: order & limit for Set operations
    return null;
  }
//
//  public Scope visitSelect(Select select, Scope scope) {
//    List<SelectItem> items = select.getSelectItems().stream()
//        .map(s->(SingleColumn)s)
//        .map(s-> new SingleColumn(rewriteExpression(s.getExpression(), scope), s.getAlias()))
//        .collect(Collectors.toList());
//
//    return createScope(new Select(select.getLocation(), select.isDistinct(), items), scope);
//  }
}
