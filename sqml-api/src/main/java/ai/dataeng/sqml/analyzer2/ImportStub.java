package ai.dataeng.sqml.analyzer2;

import static ai.dataeng.sqml.tree.name.NameCanonicalizer.LOWERCASE_ENGLISH;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

@AllArgsConstructor
public class ImportStub {
  TableEnvironment env;
  TableManager tableManager;

  public void importTable(NamePath namePath) {
    final String ordersPath = "file:///Users/henneberger/Projects/sqml-official/sqml-examples/retail/ecommerce-data/orders.json";

    TableResult tableResult = env.executeSql(
        "CREATE TABLE Orders ("
            + "  id BIGINT,"
            + "  customerid INT,"
            + "  `time` INT,"
            + "  `uuid` AS UUID(),"
            + "  entries ARRAY<"
            + "    ROW<"
            + "      productid INT, "
            + "      quantity INT, "
            + "      unit_price DOUBLE, "
            + "      discount DOUBLE"
            + "    >"
            + "  >"
            + ") WITH ("
            + "  'connector' = 'filesystem',"
            + "  'path' = '"
            + ordersPath
            + "',"
            + "  'format' = 'json'"
            + ")");

    Table orders = env.sqlQuery(
        "SELECT id, customerid, `time`,  `uuid` FROM Orders");

    NamePath ordersName = Name.of("Orders", LOWERCASE_ENGLISH).toNamePath();
    SqrlEntity decoratedOrders = new SqrlEntity(ordersName, orders);
    decoratedOrders.setPrimaryKey(List.of(Name.of("id", LOWERCASE_ENGLISH)));
    tableManager.getTables().put(ordersName, decoratedOrders);

    Table entries = env.sqlQuery(
        "SELECT o.id AS `id`, o.`uuid` as `uuid`,  e.* FROM Orders o, UNNEST(o.`entries`) e");

    NamePath entriesName = NamePath.of(
        Name.of("Orders", LOWERCASE_ENGLISH),
        Name.of("entries", LOWERCASE_ENGLISH)
    );
    SqrlEntity decoratedEntries = new SqrlEntity(entriesName, entries);

    decoratedOrders.addRelationship(Name.system("entries"), decoratedEntries);
    tableManager.getTables().put(entriesName, decoratedEntries);

  }
}
