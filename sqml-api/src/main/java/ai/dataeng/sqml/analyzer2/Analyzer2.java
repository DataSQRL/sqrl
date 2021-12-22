package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

@AllArgsConstructor
public class Analyzer2 {

  Script script;
  TableEnvironment env;
  TableManager tableManager;
  private final ConversionError.Bundle<ConversionError> errors = new ConversionError.Bundle<>();
  ImportStub importStub;
  boolean devMode;

  public void analyze() {
    Analyzer2.Visitor visitor = new Analyzer2.Visitor();
    script.accept(visitor, null);
  }

  public static class Scope {

  }

  public class Visitor extends AstVisitor<Scope, Scope> {

    @Override
    public Scope visitNode(Node node, Scope context) {
      throw new RuntimeException(
          String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Scope visitScript(Script node, Scope context) {
//      List<Node> statements = node.getStatements();
//      for (Node statement : statements) {
//        Preconditions.checkState(!(tableManager.isReadMode() && statement instanceof ImportDefinition),
//            "Read mode set before imports");
//        if (devMode && !(statement instanceof ImportDefinition)) {
//          tableManager.setReadMode();
//        }
//
//        statement.accept(this, null);
//
//      }

      return null;
    }

    @Override
    public Scope visitImportDefinition(ImportDefinition node, Scope scope) {
//      importStub.importTable(node.getNamePath());

      return null;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment node, Scope context) {
//
//      String query = rewriteQuery(node, context, false);
//
//      System.out.println(query);
//
//      Table table = env.sqlQuery(query);
//
//      SqrlTable queryEntity = new SqrlTable(node.getNamePath(), table);
//
//      List<Name> pks = extractPrimaryKey(query);
//      queryEntity.setPrimaryKey(pks);
//      System.out.println("Primary keys:" + pks);
//
//
//      String queryForMaterialize = tableManager.isReadMode()
//          ? rewriteQuery(node, context, true)
//          : query;
//
//      tableManager.setTable(node.getNamePath(), queryEntity, queryForMaterialize);
//
//      //Add relationship
//      if (node.getNamePath().getPrefix().isPresent()) {
//        SqrlTable ent = tableManager.getTable(node.getNamePath().getPrefix().get());
//        ent.addRelationship(node.getNamePath().getLast(), queryEntity);
//      }

      return null;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
//      SqrlTable ent = tableManager.getTable(node.getNamePath().getPrefix().get());
//      String expr = node.getExpression().accept(new NodeFormatter(), null);
//
//
//      Table table = ent.getTable().addColumns(expr + " AS " + node.getNamePath().getLast().getDisplay());
//      //Updates table ref
//      ent.setTable(table);

      return null;
    }
  }
}