package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.ScriptNode;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ScriptProcessorImpl implements ScriptProcessor {
  ImportProcessor importProcessor;
  QueryProcessor queryProcessor;
  ExpressionProcessor expressionProcessor;
  JoinProcessor joinProcessor;
  DistinctProcessor distinctProcessor;
  SubscriptionProcessor subscriptionProcessor;
  Namespace namespace;

  public Namespace process(ScriptNode script) {

    for (Node statement : script.getStatements()) {
      if (statement instanceof ImportDefinition) {
        importProcessor.process((ImportDefinition) statement, namespace);
      } else if (statement instanceof QueryAssignment) {
        queryProcessor.process((QueryAssignment) statement, namespace);
      } else if (statement instanceof ExpressionAssignment) {
        expressionProcessor.process((ExpressionAssignment) statement, namespace);
      } else if (statement instanceof JoinAssignment) {
        joinProcessor.process((JoinAssignment) statement, namespace);
      } else if (statement instanceof CreateSubscription) {
        subscriptionProcessor.process((CreateSubscription) statement, namespace);
      } else {
        throw new RuntimeException(String.format("Unknown statement type %s", statement.getClass().getName()));
      }
    }

    return namespace;
  }
}
