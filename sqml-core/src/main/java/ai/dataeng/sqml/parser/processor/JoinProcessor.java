package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.JoinAssignment;

public interface JoinProcessor {

  public void process(JoinAssignment statement, Namespace namespace);
}
