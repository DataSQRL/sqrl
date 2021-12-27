package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.ScriptParser;

public interface ScriptParserProvider {
  ScriptParser createScriptParser();
}
