package com.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

class ParseTest {


  private static SqrlParserImpl parser;
  private ErrorCollector error;

  final String JOIN_TYPE_TEMPLATE = "X := SELECT * FROM a %s JOIN b;";

  @BeforeAll
  public static void setup() {
    parser = new SqrlParserImpl();
  }

  @BeforeEach
  public void setupError() {
    this.error = ErrorCollector.root();
  }

  @AfterEach
  public void verify() {
    assertFalse(this.error.hasErrors(), ()-> "Test has errors: " + ErrorPrinter.prettyPrint(this.error));
  }

  @Test
  public void testJoinTypes() {
    Set<String> types = Set.of(
        "LEFT", "LEFT OUTER", "LEFT TEMPORAL", "LEFT INTERVAL",
        "RIGHT", "RIGHT OUTER", "RIGHT TEMPORAL", "RIGHT INTERVAL",
        "TEMPORAL",
        "INTERVAL",
        "INNER",
        "");

    for (String type : types) {
      parser.parse(String.format(JOIN_TYPE_TEMPLATE, type));
    }
  }

  @Test
  public void testSqlSelect() {
    parser.parse("Tbl.nested(val: Int = 5, var: String) := SELECT * FROM tbl.nested;");
  }
}