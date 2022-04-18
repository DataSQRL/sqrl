package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.scopes.TableScope;
import ai.datasqrl.validate.paths.RelativeTablePath;
import ai.datasqrl.validate.scopes.ValidatorScope;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Converts an expression from an expression assignment to a query node
 */
@AllArgsConstructor
public class ExpressionToQueryTransformer {
  private final Table contextTable;
  @Getter
  private final Map<Node, ValidatorScope> scopes = new HashMap<>();

  public Query transform(Expression expression) {
    Optional<NodeLocation> location = expression.getLocation();

    Select select = new Select(expression.getLocation(), false,
        List.of(new SingleColumn(expression)));

    TableNode tableNode = new TableNode(location, Name.SELF_IDENTIFIER.toNamePath(), Optional.empty());


    scopes.put(tableNode, new TableScope(tableNode,
        new RelativeTablePath(contextTable, Name.SELF_IDENTIFIER.toNamePath()),
        Name.SELF_IDENTIFIER));
    QuerySpecification queryBody = new QuerySpecification(location, select,
        tableNode, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty());
    return new Query(location, queryBody, Optional.empty(), Optional.empty());
  }
}
