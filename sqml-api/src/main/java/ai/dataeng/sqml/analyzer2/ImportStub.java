package ai.dataeng.sqml.analyzer2;

import static ai.dataeng.sqml.tree.name.NameCanonicalizer.LOWERCASE_ENGLISH;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.nio.file.Path;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

@AllArgsConstructor
public class ImportStub {
  TableEnvironment env;
  SqrlCatalogManager catalogManager;

  public void importTable(NamePath namePath) {
    Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
    final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
    final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);

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
            + RETAIL_DATA_DIR.resolve("orders.json")
            + "',"
            + "  'format' = 'json'"
            + ")");

    Table orders = env.sqlQuery(
        "SELECT id, customerid, `time`, `uuid` FROM Orders");
//
//    Table orders2 = env.sqlQuery(
//        "SELECT id, customerid, `time`, `uuid` FROM "+ orders);

    NamePath ordersName = Name.of("Orders", LOWERCASE_ENGLISH).toNamePath();
    SqrlTable decoratedOrders = new SqrlTable(ordersName, orders, "", List.of(Name.of("id", LOWERCASE_ENGLISH)));
//    catalogManager.addTable(decoratedOrders);

    Table entries = env.sqlQuery(
        "SELECT o.id AS `id`, o.`uuid` as `uuid`,  e.* FROM Orders o, UNNEST(o.`entries`) e");

    NamePath entriesName = NamePath.of(
        Name.of("Orders", LOWERCASE_ENGLISH),
        Name.of("entries", LOWERCASE_ENGLISH)
    );
    SqrlTable decoratedEntries = new SqrlTable(entriesName, entries, "", List.of());

    decoratedOrders.addRelColumn(Name.system("entries"), decoratedEntries);
//    catalogManager.addTable(decoratedEntries);

  }
}
