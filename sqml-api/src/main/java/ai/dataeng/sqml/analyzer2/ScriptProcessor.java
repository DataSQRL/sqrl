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
        SqrlTable table = env.addQuery(query.getNamePath(), query.getSql());
        catalogManager.addTable(query.getNamePath(), table);
        
        //Add a relationship column if present
        Optional<NamePath> prefix = query.getNamePath().getPrefix();
        if (prefix.isPresent()) {
          SqrlTable prefixTable = catalogManager.getCurrentTable(prefix.get());
          SqrlTable modifiedTable = prefixTable.addRelColumn(query.getNamePath().getLast(), table);
          //TODO: add rel is in place, make immutable?
//          catalogManager.addTable(prefix.get(), modifiedTable);
        }
      } else if (statement instanceof ExpressionAssignment) {
        ExpressionAssignment expr = (ExpressionAssignment) statement;
        Optional<NamePath> tableName = expr.getNamePath().getPrefix();
        Preconditions.checkState(tableName.isPresent(), "Cannot assign expression to schema root");
        SqrlTable table = catalogManager.getCurrentTable(tableName.get());
        SqrlTable extendedTable = table.addColumn(expr.getNamePath().getLast(), expr.getSql());
        catalogManager.addTable(tableName.get(), extendedTable);
      } else {
        throw new RuntimeException(String.format("Unknown statement type %s", statement.getClass().getName()));
      }
    }
  }
}
