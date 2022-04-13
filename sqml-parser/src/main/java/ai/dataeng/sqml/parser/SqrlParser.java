package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.ParsingOptions.DecimalLiteralTreatment;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.SqrlStatement;
import ai.dataeng.sqml.tree.Statement;

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