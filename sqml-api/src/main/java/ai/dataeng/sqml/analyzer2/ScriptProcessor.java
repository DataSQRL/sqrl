package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ScriptProcessor {
  SqrlCatalogManager catalogManager;
  SqrlEnvironment env;

  public void process(Script script) {

    for (Node statement : script.getStatements()) {
      if (statement instanceof ImportDefinition) {
        env.addImport(((ImportDefinition)statement).getNamePath());
      } else if (statement instanceof QueryAssignment) {
        QueryAssignment query = (QueryAssignment) statement;
        SqrlTable2 table = env.addQuery(query.getNamePath(), query.getSql());
        catalogManager.addTable(table);
        
        //Add a relationship column if present
        Optional<NamePath> prefix = query.getNamePath().getPrefix();
        if (prefix.isPresent()) {
          SqrlTable2 prefixTable = catalogManager.getCurrentTable(prefix.get());
          env.addRelationshipToSchema(prefixTable, query.getNamePath().getLast(), table);
//          SqrlTable modifiedTable = prefixTable.addRelColumn();
          //TODO: add rel is in place, make immutable?
//          catalogManager.addTable(prefix.get(), modifiedTable);
        }
      } else if (statement instanceof ExpressionAssignment) {
        ExpressionAssignment expr = (ExpressionAssignment) statement;
        Optional<NamePath> tableName = expr.getNamePath().getPrefix();
        Preconditions.checkState(tableName.isPresent(), "Cannot assign expression to schema root");
        SqrlTable2 table = catalogManager.getCurrentTable(tableName.get());
        SqrlTable2 extendedTable = env.addColumn(table, expr.getNamePath().getLast(), expr.getSql());
        catalogManager.addTable(extendedTable);
      } else {
        throw new RuntimeException(String.format("Unknown statement type %s", statement.getClass().getName()));
      }
    }
  }
}
