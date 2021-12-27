package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.tree.ScriptNode;

public class ScriptParserImpl implements ScriptParser {

  @Override
  public ScriptNode parse(SqmlScript script) {
    SqmlParser sqmlParser = SqmlParser.newSqmlParser();
    return sqmlParser.parse(script.getScriptContent());
  }
}
