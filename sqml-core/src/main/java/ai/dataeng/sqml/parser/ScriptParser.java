package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.tree.ScriptNode;

public interface ScriptParser {

  ScriptNode parse(SqmlScript script);
}
