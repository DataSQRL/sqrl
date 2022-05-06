package ai.datasqrl.plan.local.transpiler.transforms;

import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class ExtractSubQuery {

  public static QuerySpecification extract(FunctionCall node, Name fieldName, NamePath tablePath) {
    Identifier rewrittenIdentifier = new Identifier(Optional.empty(),
        fieldName.toNamePath());
    FunctionCall rewritten = new FunctionCall(node.getLocation(), node.getNamePath(),
        List.of(rewrittenIdentifier),
        node.isDistinct(), node.getOver());

    // Build a query using the path we need to expand and
    QuerySpecification spec = new QuerySpecification(
        Optional.empty(),
        new Select(List.of(new SingleColumn(rewritten))),
        new TableNode(Optional.empty(),
            tablePath, Optional.empty()),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    return spec;
  }
}
