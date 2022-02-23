package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.tree.ScriptNode;

public interface ScriptParser {

  ScriptNode parse(SqrlScript script);
}
