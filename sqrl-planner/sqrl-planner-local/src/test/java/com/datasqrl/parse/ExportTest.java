/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.error.ErrorCollector;
import java.util.List;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlStatement;
import org.junit.jupiter.api.Test;

class ExportTest {

  @Test
  public void exportTest() {
    SqrlParser parser = new SqrlParserImpl();
    // cannot use ANTLR defined keywords e.g. 'table' or 'source'
    String sqrl = "EXPORT UserAlerts TO file-output.Alerts";
    SqrlStatement sqrlStatement = parser.parseStatement(sqrl);
    assertTrue(sqrlStatement instanceof SqrlExportDefinition);
    SqrlExportDefinition exportDefinition = (SqrlExportDefinition) sqrlStatement;

    assertEquals(exportDefinition.getTablePath().names, List.of("UserAlerts"));
    assertEquals(exportDefinition.getSinkPath().names, List.of("file-output", "Alerts"));
  }
}