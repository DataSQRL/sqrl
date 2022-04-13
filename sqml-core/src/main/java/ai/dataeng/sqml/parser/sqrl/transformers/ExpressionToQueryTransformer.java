package ai.dataeng.sqml.parser.sqrl.transformers;

import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
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
