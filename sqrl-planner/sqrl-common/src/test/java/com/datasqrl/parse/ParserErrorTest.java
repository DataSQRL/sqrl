/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.util.SnapshotTest;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
        .withSource(str);

//    errorCollector.registerHandler(ParsingException.class, new ParsingExceptionHandler());

    SqrlParser parser = new SqrlParserImpl();

    try {
      ScriptNode n = parser.parse(str, errorCollector);
      fail();
    } catch (CollectedException e) {
      //Do nothing, error should be collected
    }

    snapshot.addContent(ErrorPrinter.prettyPrint(errorCollector));
  }
}
