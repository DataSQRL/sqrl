package ai.dataeng.sqml.rewrite;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AddColumnsFromStatistics extends ScriptRewrite {
  private final Metadata metadata;

  public AddColumnsFromStatistics(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public Script rewrite(Script script) {
    List<Node> statements = new ArrayList<>();
    for (Map.Entry<QualifiedName, Type> entry : metadata.getStatisticsProvider().getColumns().entrySet()) {
      QualifiedName name = entry.getKey();
      Type type = entry.getValue();
      Assign assign = new Assign(null, name,
          new ExpressionAssignment(Optional.empty(),
              new Identifier(name.getSuffix(), type.toString())));
      statements.add(assign);
    }
    statements.addAll(script.getStatements());

    return new Script(script.getLocation().get(), statements);
  }
}
