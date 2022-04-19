package ai.datasqrl;

import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration.Dialect;
import ai.datasqrl.parse.operator.C360Test;
import ai.datasqrl.server.ScriptDeployment;
import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import com.google.common.collect.ImmutableList;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Test2 {

  private VertxInternal vertx;
  private Environment env;

  @BeforeEach
  public void setup() throws IOException {
    VertxOptions vertxOptions = new VertxOptions();
    this.vertx = (VertxInternal) Vertx.vertx(vertxOptions);

//    FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);

    env = Environment.create(settings);

//    env.getDatasetRegistry().addOrUpdateSource("c360", dd, ErrorCollector.root());


    String ds2Name = "ecommerce-data";
    DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
        .uri(C360Test.RETAIL_DATA_DIR.toAbsolutePath().toString())
        .build();
    env.getDatasetRegistry().addOrUpdateSource(ds2Name, fileConfig, ErrorCollector.root());
//    assertFalse(errors.isFatal());

//    registerDatasets();
  }

  @Test
  public void test() {
    //c360, test import all the way through to query
    run(
        "IMPORT ecommerce-data.Customer;\n"
             + "IMPORT ecommerce-data.Product;\n"
             + "IMPORT ecommerce-data.Orders;\n"
//            + "\n"
//            //TODO: 1. move some conditions to fields to be filtered on by db
//            + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
//            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
////            TODO: Flink requires order to be a timestamp and required for rank
//            + "-- Compute useful statistics on orders\n"
//            + "Orders.entries.discount := coalesce(discount, 0.0);\n"
//            + "Orders.entries.total := quantity * unit_price - discount;\n"
//            + "Orders.total := sum(entries.total);\n"
//            + "Orders.total_savings := sum(entries.discount);\n"
//            + "Orders.total_entries := count(entries);\n"
//            + "\n"
//            + "\n"
//            + "-- Relate Customer to Orders and compute a customer's total order spent\n"
//            + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
//            + "Orders.entries.product := JOIN Product ON Product.productid = _.productid LIMIT 1;"
//            + "Customer.recent_products := SELECT productid, product.category AS category,\n"
//            + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
//            + "                            FROM _.orders.entries e\n"
////            TODO: Interval format not coercing
//            + "                            WHERE parent.time > now()\n"
//            + "                            GROUP BY productid, category;\n"
////            TODO: Order by non-timestmap
////            + "                            ORDER BY num_orders DESC, quantity DESC;\n"
////            + "\n"
//            + "Customer.recent_products_categories :=\n"
//            + "                     SELECT category, count(*) AS num_products\n"
//            + "                     FROM _.recent_products\n"
//            + "                     GROUP BY category;"
//////            + "                     ORDER BY num_products;\n"
////            + "\n"
//            + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp.category=_.category;\n"
//            + "\n"
//            + "-- Aggregate customer spending by month and product category for the 'spending history' feature\n"
//            + "Customer._spending_by_month_category :=\n"
//            + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
//            + "                            product.category AS category,\n"
//            + "                            sum(total) AS total,\n"
//            + "                            sum(discount) AS savings\n"
//            + "                     FROM _.orders.entries\n"
//            + "                     GROUP BY month, category;"
////            + "                      ORDER BY month DESC;\n"
////            + "\n"
//            + "Customer.spending_by_month :=\n"
//            + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
//            + "                    FROM _._spending_by_month_category\n"
//            + "                    GROUP BY month;"
//////            + "                    ORDER BY month DESC;\n"
//            + "Customer.spending_by_month.categories :=\n"
//            + "    JOIN _.parent._spending_by_month_category c ON c.month = _.month;\n"
//            + "\n"
//            + "/* Compute w/w product sales volume increase average over a month\n"
//            + "   These numbers are internal to determine trending products */\n"
//////            TODO: Bug with join traversals, need to add criteria
//            + "Product.order_entries := JOIN Orders o JOIN o.entries e ON e.productid = _.productid;\n"
//            + "Product._sales_last_week := SELECT SUM(e.quantity)\n"
//            + "                          FROM _.order_entries e\n"
//            + "                          WHERE e.parent.time > now();\n"
//////            + "\n"
//            + "Product._sales_last_month := SELECT SUM(e.quantity)\n"
//            + "                          FROM _.order_entries e\n"
//            + "                          WHERE e.parent.time > now();\n"
//////            + "\n"
//            + "Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n"
//////            + "\n"
//////            + "-- Determine trending products for each category\n"
//            + "Category := SELECT DISTINCT category AS name FROM Product;\n"
//            + "Category.products := JOIN Product ON _.name = Product.category;\n"
//            + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase > 0\n"
//            + "                     ORDER BY p._last_week_increase DESC"
//            + "                     LIMIT 10;\n"
//            + "\n"
//            + "/* Determine customers favorite categories by total spent\n"
//            + "   In combination with trending products this is used for the product recommendation feature */\n"
//            + "Customer.favorite_categories := SELECT s.category as category_name,\n"
//            + "                                        sum(s.total) AS total\n"
//            + "                                FROM _._spending_by_month_category s\n"
//            + "                                WHERE s.month >= now()\n"
//            + "                                GROUP BY category_name"
////            + "                                ORDER BY total DESC"
//            + "                                LIMIT 5;\n"
////            + "\n"
//            + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n"
//            + "\n"
//            + "-- Create subscription for customer spending more than $100 so we can send them a coupon --\n"
//            + "\n"
//             TODO: Not yet implemented
//            + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS\n"
//            + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >= 100;"
    );

//    System.out.println(script.getGraphQL().execute("query {orders { data { total, total_savings, total_entries } } }"));

  }

  private void run(String script) {
    ErrorCollector errorCollector = ErrorCollector.root();
    try {
      ScriptBundle.Config bundle = ScriptBundle.Config.builder()
          .name(C360Test.RETAIL_SCRIPT_NAME)
          .scripts(ImmutableList.of(
              SqrlScript.Config.builder()
                  .name(C360Test.RETAIL_SCRIPT_NAME)
                  .main(true)
                  .content(script)
                  .inputSchema(Files.readString(C360Test.RETAIL_IMPORT_SCHEMA_FILE))
                  .build()
          ))
          .build();
      env.deployScript(bundle, errorCollector);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
