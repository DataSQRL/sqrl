package ai.datasqrl.parse;

import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.ParsingOptions.DecimalLiteralTreatment;
import ai.datasqrl.parse.tree.SqrlStatement;

public class SqrlParser {

  private final SqlParser parser;
  private final ParsingOptions parsingOptions;

  public SqrlParser() {
    parser = new SqlParser(new SqlParserOptions());
    parsingOptions = ParsingOptions.builder()
        .setDecimalLiteralTreatment(DecimalLiteralTreatment.AS_DOUBLE)
        .build();
  }

  public ScriptNode parse(String script) {
    return parser.createScript(script, parsingOptions);
  }

  public static SqrlParser newParser() {
    return new SqrlParser();
  }

  public SqrlStatement parseStatement(String statement) {
    return (SqrlStatement)parser.createStatement(statement, parsingOptions);
  }
}