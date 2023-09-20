package com.datasqrl.parse;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

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
      parser.parse(String.format(JOIN_TYPE_TEMPLATE, type), error);
    }
  }

  @Test
  public void testSqlSelect() {
    parser.parse("Table.nested(val: Int = 5, var: String) := SELECT * FROM table.nested", error);
  }

  @Test
  public void testFnc() {
    parser.parse("Table.nested() := SELECT * FROM a.b(id, e.id).c.d.e", error);
  }
}