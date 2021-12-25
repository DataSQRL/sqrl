package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;

public interface ExpressionProcessor {

  public void process(ExpressionAssignment statement, Namespace namespace);// {
//        ExpressionAssignment expr = (ExpressionAssignment) statement;
//        Optional<NamePath> tableName = expr.getNamePath().getPrefix();
//        Preconditions.checkState(tableName.isPresent(), "Cannot assign expression to schema root");
//        SqrlTable2 table = catalogManager.getCurrentTable(tableName.get());
//        SqrlTable2 extendedTable = env.addColumn(table, expr.getNamePath().getLast(), expr.getSql());
//        catalogManager.addTable(extendedTable);
//  }
}
