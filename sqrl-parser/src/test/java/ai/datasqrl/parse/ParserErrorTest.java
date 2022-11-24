package ai.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.fail;

import ai.datasqrl.config.error.ErrorCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ParserErrorTest {
  ErrorCollector errorCollector;

  @BeforeEach
  public void before() {
    errorCollector = ErrorCollector.root();
  }

  @Test
  public void astError() {
    handle("UNKNOWNTOKEN package;");

    System.out.println(errorCollector.getAll());
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
    System.out.println(errorCollector.getAll());
  }
}
