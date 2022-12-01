package ai.datasqrl.parse;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorPrinter;
import ai.datasqrl.config.error.SourceMapImpl;
import ai.datasqrl.util.SnapshotTest;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.fail;

public class ParserErrorTest {
  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void before(TestInfo testInfo) {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @AfterEach
  public void after() {
    snapshot.createOrValidate();
  }

  @Test
  public void topLevelUnknownToken() {
    handle("UNKNOWNTOKEN package;");
  }

  @Test
  public void invalidIdentifier() {
    handle("Test$0.example := SELECT * FROM x;");
  }

  @Test
  public void backtickIdentifier() {
    handle("Test.example := SELECT * FROM `x$0`;");
  }

  @Test
  public void quotedIdentifier() {
    handle("Test.example := SELECT * FROM \"x$0\";");
  }

  @Test
  public void digitIdentifier() {
    handle("Test.example := SELECT * FROM 0test;");
  }

  @Test
  public void reservedWord() {
    handle("Test.example := SELECT AS AS AS, * FROM t;");
  }

  public void handle(String str) {
    ErrorCollector errorCollector = ErrorCollector.root()
        .sourceMap(new SourceMapImpl(str));

    errorCollector.registerHandler(ParsingException.class, new ParsingExceptionHandler());

    SqrlParser parser = SqrlParser.newParser();

    try {
      ScriptNode n = parser.parse(str);
      fail();
    } catch (ParsingException e) {
      errorCollector.handle(e);
    }

    snapshot.addContent(ErrorPrinter.prettyPrint(errorCollector));
  }
}
