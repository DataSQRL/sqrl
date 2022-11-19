package ai.datasqrl.parse;

import ai.datasqrl.parse.ParsingOptions.DecimalLiteralTreatment;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;

/**
 * Parses SQRL scripts or statements using {@link SqrlParser} and holds the {@link ParsingOptions} configuration
 * used to control how the script is parsed.
 */
public class ConfiguredSqrlParser {

  private final SqrlParser parser;
  private final ParsingOptions parsingOptions;

  //TODO: Add warnings
  public ConfiguredSqrlParser(WarningsCollector errorCollector) {
    parser = new SqrlParser(new SqrlParserOptions());
    parsingOptions = ParsingOptions.builder()
        .setWarningConsumer((e) ->

            errorCollector.warn(e.getLineNumber(),e.getColumnNumber(),e.getWarning()))
        .setDecimalLiteralTreatment(DecimalLiteralTreatment.AS_DOUBLE)
        .build();
  }

  public ScriptNode parse(String script) {
    System.out.println(script);
    return parser.createScript(script, parsingOptions);
  }

  public SqrlStatement parseStatement(String statement) {
    return parser.createStatement(statement, parsingOptions);
  }

  public static ConfiguredSqrlParser newParser(WarningsCollector warningsCollector) {
    return new ConfiguredSqrlParser(warningsCollector);
  }

}