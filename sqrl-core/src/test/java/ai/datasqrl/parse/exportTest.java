package ai.datasqrl.parse;

import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.SqrlStatement;
import org.junit.jupiter.api.Test;
import static org.junit.Assert.*;

class exportTest {

    @Test
    public void ExportTest() {
        SqrlParser parser = new SqrlParser(new SqrlParserOptions());
        // cannot use ANTLR defined keywords e.g. 'table' or 'source'
        SqrlStatement sqrlStatement = parser.createStatement("EXPORT UserAlerts TO file-output.Alerts", ParsingOptions.builder().build());
        //System.out.println(NodeFormatter.accept(sqrlStatement));
        assertEquals("EXPORT UserAlerts TO file-output.Alerts",
                      NodeFormatter.accept(sqrlStatement));
    }
}