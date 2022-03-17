package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.tree.ScriptNode;

public class ScriptParserImpl implements ScriptParser {

  @Override
  public ScriptNode parse(SqrlScript script) {
    SqmlParser sqmlParser = SqmlParser.newSqmlParser();
    return sqmlParser.parse(script.getContent());
  }
}
