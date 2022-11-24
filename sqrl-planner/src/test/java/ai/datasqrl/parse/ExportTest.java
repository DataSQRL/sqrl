package ai.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.sql.SqrlStatement;
import org.junit.jupiter.api.Test;

class ExportTest {

  @Test
  public void exportTest() {
    SqrlParser parser = new SqrlParser();
    // cannot use ANTLR defined keywords e.g. 'table' or 'source'
    SqrlStatement sqrlStatement = parser.parseStatement("EXPORT UserAlerts TO file-output.Alerts");
    assertTrue(sqrlStatement instanceof ExportDefinition);
    ExportDefinition exportDefinition = (ExportDefinition) sqrlStatement;

    assertEquals(exportDefinition.getTablePath().names, List.of("UserAlerts"));
    assertEquals(exportDefinition.getSinkPath().names, List.of("file-output", "Alerts"));
  }
}