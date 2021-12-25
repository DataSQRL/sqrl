package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Statement;

public interface ImportProcessor {

  public void process(ImportDefinition statement, Namespace namespace);
}
