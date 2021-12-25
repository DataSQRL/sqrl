package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;

public interface DistinctProcessor {

  public void process(ImportDefinition statement, Namespace namespace);
}
