package ai.dataeng.sqml.parser.sqrl;

import static ai.dataeng.sqml.parser.operator.C360Test.*;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.parser.Script;
import ai.dataeng.sqml.parser.operator.C360Test;
import ai.dataeng.sqml.parser.operator.DefaultTestSettings;
import com.google.common.collect.ImmutableList;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
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

    env.getDatasetRegistry().addOrUpdateSource(dd, ErrorCollector.root());
//    registerDatasets();
  }


  private void registerDatasets() {
//    ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();

    String ds2Name = "ecommerce-data";
    FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
        .uri(C360Test.RETAIL_DATA_DIR.toAbsolutePath().toString())
        .name(ds2Name)
        .build();
    env.getDatasetRegistry().addOrUpdateSource(fileConfig, ErrorCollector.root());
//    assertFalse(errors.isFatal());


    //Needs some time to wait for the flink pipeline to compile data
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  @Test
  public void test() {
    //c360, test import all the way through to query
    Script script = run(
        "IMPORT ecommerce-data.Customer;\n"
            + "IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Orders;\n"
            + "\n"
            + "\n"
            + "-- Compute useful statistics on orders\n"
            + "Orders.entries.discount := coalesce(discount, 0.0);\n"
            + "Orders.entries.total := quantity * unit_price - discount;\n"
            + "Orders.total := sum(entries.total);\n"
            + "Orders.total_savings := sum(entries.discount);\n"
            + "Orders.total_entries := count(entries);\n"
            + "\n"
            + "\n"
            + "-- Relate Customer to Orders and compute a customer's total order spent\n"
            + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
            + "Customer.total_orders := sum(orders.total);\n"
            + "\n"
            + "-- Aggregate all products the customer has ordered for the 'order again' feature\n"
            + "Orders.entries.product := JOIN Product ON Product.productid = _.productid LIMIT 1;\n"
            + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
            + "\n"
            + "Customer.recent_products := SELECT productid, product.category AS category,\n"
            + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
            + "                            FROM _.orders.entries\n"
            + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
            + "                            GROUP BY productid, category\n"
            + "                            ORDER BY num_orders DESC, quantity DESC;\n"
            + "\n"
            + "Customer.recent_products_categories :=\n"
            + "                     SELECT category, count(*) AS num_products\n"
            + "                     FROM _.recent_products\n"
            + "                     GROUP BY category ORDER BY num_products;\n"
            + "\n"
            + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp.category=_.category;\n"
            + "\n"
            + "-- Aggregate customer spending by month and product category for the 'spending history' feature\n"
            + "Customer._spending_by_month_category :=\n"
            + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
            + "                            product.category AS category,\n"
            + "                            sum(total) AS total,\n"
            + "                            sum(discount) AS savings\n"
            + "                     FROM _.orders.entries\n"
            + "                     GROUP BY month, category ORDER BY month DESC;\n"
            + "\n"
            + "Customer.spending_by_month :=\n"
            + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
            + "                    FROM _._spending_by_month_category\n"
            + "                    GROUP BY month ORDER BY month DESC;\n"
            + "Customer.spending_by_month.categories :=\n"
            + "    JOIN _.parent._spending_by_month_category c ON c.month = _.month;\n"
            + "\n"
            + "/* Compute w/w product sales volume increase average over a month\n"
            + "   These numbers are internal to determine trending products */\n"
//            + "Product._sales_last_week := SELECT SUM(e.quantity)\n"
//            + "                          FROM _.order_entries e\n"
//            + "                          WHERE e.parent.time > now() - INTERVAL 1 WEEK;\n"
//            + "\n"
//            + "Product._sales_last_month := SELECT SUM(e.quantity)\n"
//            + "                          FROM _.order_entries e\n"
//            + "                          WHERE e.parent.time > now() - INTERVAL 4 WEEK;\n"
//            + "\n"
//            + "Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n"
//            + "\n"
//            + "-- Determine trending products for each category\n"
//            + "Category := SELECT DISTINCT category AS name FROM Product;\n"
//            + "Category.products := JOIN Product ON _.name = Product.category;\n"
//            + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase > 0\n"
//            + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
//            + "\n"
//            + "/* Determine customers favorite categories by total spent\n"
//            + "   In combination with trending products this is used for the product recommendation feature */\n"
//            + "Customer.favorite_categories := SELECT s.category as category_name,\n"
//            + "                                        sum(s.total) AS total\n"
//            + "                                FROM _._spending_by_month_category s\n"
//            + "                                WHERE s.month >= now() - INTERVAL 1 YEAR\n"
//            + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n"
//            + "\n"
//            + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n"
//            + "\n"
//            + "-- Create subscription for customer spending more than $100 so we can send them a coupon --\n"
            + "\n"
    );

    System.out.println(script.getGraphQL().execute("query {orders { data { total, total_savings, total_entries } } }"));

  }

  private Script run(String script) {
    try {
      ScriptBundle bundle = ScriptBundle.Config.builder()
          .name(RETAIL_SCRIPT_NAME)
          .scripts(ImmutableList.of(
              SqrlScript.Config.builder()
                  .name(RETAIL_SCRIPT_NAME)
                  .main(true)
                  .content(script)
                  .inputSchema(Files.readString(RETAIL_IMPORT_SCHEMA_FILE))
                  .build()
          ))
          .build().initialize(ErrorCollector.root());
      return env.compile(ScriptDeployment.of(bundle));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
