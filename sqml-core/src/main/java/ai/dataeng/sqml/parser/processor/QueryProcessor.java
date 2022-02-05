package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.QueryAssignment;

public interface QueryProcessor {

  void process(QueryAssignment statement, Namespace namespace);
}
