package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;

public interface ExpressionProcessor {

  public void process(ExpressionAssignment statement, Namespace namespace);
}
