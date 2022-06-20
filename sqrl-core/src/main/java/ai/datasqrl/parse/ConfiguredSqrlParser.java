package ai.datasqrl.parse;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.ParsingOptions.DecimalLiteralTreatment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SqrlStatement;

/**
 * Parses SQRL scripts or statements using {@link SqrlParser} and holds the {@link ParsingOptions} configuration
 * used to control how the script is parsed.
 */
public class ConfiguredSqrlParser {

  private final SqrlParser parser;
  private final ParsingOptions parsingOptions;

  public ConfiguredSqrlParser(ErrorCollector errorCollector) {
    parser = new SqrlParser(new SqrlParserOptions());
    parsingOptions = ParsingOptions.builder()
        .setWarningConsumer((e) ->
            errorCollector.warn(e.getLineNumber(),e.getColumnNumber(),e.getWarning()))
        .setDecimalLiteralTreatment(DecimalLiteralTreatment.AS_DOUBLE)
        .build();
  }

  public ScriptNode parse(String script) {
    return parser.createScript(script, parsingOptions);
  }

  public SqrlStatement parseStatement(String statement) {
    return parser.createStatement(statement, parsingOptions);
  }

  public static ConfiguredSqrlParser newParser(ErrorCollector errorCollector) {
    return new ConfiguredSqrlParser(errorCollector);
  }

}