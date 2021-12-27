package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.ParsingOptions.DecimalLiteralTreatment;
import ai.dataeng.sqml.tree.ScriptNode;

public class SqmlParser {

  private final SqlParser parser;
  private final ParsingOptions parsingOptions;

  public SqmlParser() {
    parser = new SqlParser(new SqlParserOptions());
    parsingOptions = ParsingOptions.builder()
        .setDecimalLiteralTreatment(DecimalLiteralTreatment.AS_DOUBLE)
        .build();
  }

  public ScriptNode parse(String script) {
    return parser.createScript(script, parsingOptions);
  }

  public static SqmlParser newSqmlParser() {
    return new SqmlParser();
  }
}