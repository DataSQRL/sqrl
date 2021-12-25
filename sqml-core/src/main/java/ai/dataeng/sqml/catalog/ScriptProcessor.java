package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.tree.ScriptNode;

public interface ScriptProcessor {

  Namespace process(ScriptNode scriptNode);
}
