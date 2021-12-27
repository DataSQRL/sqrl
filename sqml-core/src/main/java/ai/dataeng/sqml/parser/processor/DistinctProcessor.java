package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.DistinctAssignment;

public interface DistinctProcessor {

  public void process(DistinctAssignment statement, Namespace namespace);
}
