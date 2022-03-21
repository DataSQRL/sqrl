package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.ScriptNode;

/**
 * Creates a schema and a set of qualified unsqrled sql nodes
 */
public interface ScriptProcessor {

  Namespace process(ScriptNode scriptNode);
}
