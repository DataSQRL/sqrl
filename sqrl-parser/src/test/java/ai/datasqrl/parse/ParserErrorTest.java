package ai.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.fail;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.util.SnapshotTest;
import org.apache.calcite.rel.core.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ParserErrorTest {
  ErrorCollector errorCollector;
  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void before(TestInfo testInfo) {
    errorCollector = ErrorCollector.root();
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @AfterEach
  public void after() {
    errorCollector.log();
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

  public void handle(String str) {
    errorCollector.registerHandler(ParsingException.class, new ParsingExceptionHandler());

    SqrlParser parser = SqrlParser.newParser();

    try {
      parser.parse(str);
      fail();
    } catch (ParsingException e) {
      errorCollector.handle(e);
    }
    snapshot.addContent(errorCollector.getAll().toString());
  }
}
