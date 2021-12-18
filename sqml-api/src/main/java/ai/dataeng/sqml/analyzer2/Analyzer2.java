package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.AstVisitor;
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
import org.apache.flink.table.api.TableResult;

@AllArgsConstructor
public class Analyzer2 {

  Script script;
  TableEnvironment env;
  TableManager tableManager;
  private final ConversionError.Bundle<ConversionError> errors = new ConversionError.Bundle<>();

  public void analyze() {
    Analyzer2.Visitor visitor = new Analyzer2.Visitor();
    script.accept(visitor, null);
  }

  public static class Scope {

  }

  public class Visitor extends AstVisitor<Scope, Scope> {

    @Override
    public Scope visitNode(Node node, Scope context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Scope visitScript(Script node, Scope context) {
      List<Node> statements = node.getStatements();
      for (Node statement : statements) {
        statement.accept(this, null);
      }

      return null;
    }

    @Override
    public Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      final String ordersPath = "file:///Users/henneberger/Projects/sqml-official/sqml-examples/retail/ecommerce-data/orders.json";

      TableResult tableResult = env.executeSql(
          "CREATE TABLE Orders ("
              + "  id BIGINT,"
              + "  customerid INT,"
              + "  `time` INT,"
              + "  `proctime` AS PROCTIME(),"
//              + "  `uuid` AS UUID(),"
              + "  entries ARRAY<ROW<productid INT, quantity INT, unit_price INT, discount INT>>"
              + ") WITH ("
              + "  'connector' = 'filesystem',"
              + "  'path' = '"
              + ordersPath
              + "',"
              + "  'format' = 'json'"
              + ")");


      Table orders = env.sqlQuery("SELECT id, customerid, `time`, `proctime`, UUID() as `uuid`, entries FROM Orders");
      SqrlEntity decoratedOrders = new SqrlEntity(orders);
//      env.getCatalog(env.getCurrentCatalog()).get()
//              .getTable()

      tableManager.getTables().put(Name.system("Orders"), decoratedOrders);
      System.out.println(orders.getResolvedSchema());

      Table entries = env.sqlQuery("SELECT o.id AS `id`, o.uuid AS `uuid`, o.proctime, e.* FROM "+orders+" o, UNNEST(o.`entries`) e");
      System.out.println(entries.getResolvedSchema());
      SqrlEntity decoratedEntries = new SqrlEntity(entries);

      decoratedOrders.addRelationship(Name.system("entries"), decoratedEntries);
      tableManager.getTables().put(Name.system("Orders.entries"), decoratedEntries);

      return null;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment node, Scope context) {

      String CustomerOrderStats = "SELECT customerid, count(*) as num_orders\n"
          + "                      FROM "+tableManager.getTables().get(Name.system("Orders")).getTable()+"\n"
          + "                      GROUP BY customerid";
      NodeFormatter nf = new NodeFormatter();
      String query = node.getQuery().accept(nf, null);
      System.out.println(query);
      Table queryAssignment = env.sqlQuery(CustomerOrderStats);
      SqrlEntity assn = new SqrlEntity(queryAssignment);

      //Extract PK
      assn.setPrimaryKey(List.of(Name.system("customerid")));

      tableManager.getTables().put(Name.system(node.getNamePath().toString()), assn);

      return null;
    }
  }
}