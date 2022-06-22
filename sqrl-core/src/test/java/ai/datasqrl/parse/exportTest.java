package ai.datasqrl.parse;

import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.SqrlStatement;
import org.junit.jupiter.api.Test;

class exportTest {

    @Test
    public void exportTest() {
        SqrlParser parser = new SqrlParser(new SqrlParserOptions());
        // cannot use ANTLR defined keywords e.g. 'table' or 'source'
        SqrlStatement sqrlStatement = parser.createStatement("EXPORT data1.json TO sink1.csv", ParsingOptions.builder().build());
        System.out.println(NodeFormatter.accept(sqrlStatement));
    }
}