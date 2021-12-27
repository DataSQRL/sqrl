package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ScriptNode;

public interface ScriptProcessor {

  Namespace process(ScriptNode scriptNode);
}
