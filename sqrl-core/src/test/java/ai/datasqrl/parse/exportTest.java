package ai.datasqrl.parse;

import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.SqrlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExportTest {

    @Test
    public void exportTest() {
        SqrlParser parser = new SqrlParser(new SqrlParserOptions());
        // cannot use ANTLR defined keywords e.g. 'table' or 'source'
        SqrlStatement sqrlStatement = parser.createStatement("EXPORT UserAlerts TO file-output.Alerts", ParsingOptions.builder().build());
        //System.out.println(NodeFormatter.accept(sqrlStatement));
        Assertions.assertEquals("EXPORT UserAlerts TO file-output.Alerts", NodeFormatter.accept(sqrlStatement));
    }
}