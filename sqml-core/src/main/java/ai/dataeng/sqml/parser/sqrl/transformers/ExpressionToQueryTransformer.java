package ai.dataeng.sqml.parser.sqrl.transformers;

import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class ExpressionToQueryTransformer {

  public Query transform(Expression expression) {
    Optional<NodeLocation> location = expression.getLocation();
    QuerySpecification queryBody = toQuerySpecification(expression);
    Query query = new Query(location, queryBody, Optional.empty(), Optional.empty());
    return query;
  }

  private QuerySpecification toQuerySpecification(Expression expression) {
    Optional<NodeLocation> location = expression.getLocation();
    QuerySpecification queryBody = new QuerySpecification(location, selectList(expression),
        relationOf("_", location), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty());
    return queryBody;
  }

  private TableNode relationOf(String relation,
      Optional<NodeLocation> location) {
    TableNode tableNode = new TableNode(location, NamePath.of(relation), Optional.empty());
    return tableNode;
  }

  private Select selectList(Expression expression) {
    Select select = new Select(expression.getLocation(), false, Optional.empty(),
        List.of(new SingleColumn(expression)));
    return select;
  }
}
