package ai.datasqrl.transform;

import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.name.Name;
import java.util.List;
import java.util.Optional;

/**
 * Converts an expression from an expression assignment to a query node
 */
public class ExpressionToQueryTransformer {

  public Query transform(Expression expression) {
    Optional<NodeLocation> location = expression.getLocation();

    Select select = new Select(expression.getLocation(), false,
        List.of(new SingleColumn(expression)));

    TableNode tableNode = new TableNode(location, Name.SELF_IDENTIFIER.toNamePath(), Optional.empty());

    QuerySpecification queryBody = new QuerySpecification(location, select,
        tableNode, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty());
    return new Query(location, queryBody, Optional.empty(), Optional.empty());
  }
}
