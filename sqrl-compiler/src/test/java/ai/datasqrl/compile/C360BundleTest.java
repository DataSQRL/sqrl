package ai.datasqrl.compile;


import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.DiscoveryConfiguration.MetaData;
import ai.datasqrl.util.JDBCTestDatabase;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class C360BundleTest {
  JDBCTestDatabase testDatabase = new JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine.POSTGRES);

  String c360Script = "IMPORT ecommerce-data.Customer;\n"
      + "IMPORT ecommerce-data.MyFunction;\n"
      + "IMPORT ecommerce-data.Product;\n"
      + "IMPORT ecommerce-data.Orders;"
      + "Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n"
      + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
      + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
      + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
      + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
      + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid"
      + "  ORDER BY e.discount DESC;\n"
      + "Orders.entries.discount := COALESCE(discount, 0.0)\n;"
      + "Orders.entries.test := MyFunction(quantity)\n;";

  @Test
  @SneakyThrows
  public void testByoPagedSchema() {
    Path dest = copyBundle(c360Script);
    ai.datasqrl.compile.Compiler compiler = new ai.datasqrl.compile.Compiler();
    compiler.run(dest.resolve("build/"), Optional.of(dest.resolve("schema.graphqls")),
        Optional.of(testDatabase.getJdbcConfiguration().getDatabase(MetaData.DEFAULT_DATABASE)));

    HttpResponse<String> s = compiler.testQuery("{\n"
        + "\n"
        + "  Orders{\n"
        + "    entries(limit:10, offset:0) {\n"
        + "      unit_price\n"
        + "      product {\n"
        + "        description\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}");

    System.out.println(s.body());
    System.out.println(s.headers());
    System.out.println(s.statusCode());
// Uncomment to test graphql
//    while(true) {
//      Thread.sleep(10);
//    }
  }

  @SneakyThrows
  private Path copyBundle(String c360Script) {
    Path dest = Files.createTempDirectory("c360bundle");
    dest.toFile().deleteOnExit();
    Path src = Path.of("src/test/resources/c360bundle/");

    copyDirectory(src, dest);

    Files.write(dest.resolve("build/").resolve("main.sqrl"), c360Script.getBytes(StandardCharsets.UTF_8));
    dest.resolve("build/").resolve("main.sqrl").toFile().deleteOnExit();
    return dest;
  }

  @SneakyThrows
  public static void copyDirectory(Path src, Path dest) {
    Files.walk(src)
        .forEach(source -> {
          try {
            Path targetPath = dest.resolve(src.relativize(source));
            Files.copy(source, targetPath, StandardCopyOption.REPLACE_EXISTING);
            targetPath.toFile().deleteOnExit();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        });
  }

//  @SneakyThrows
//  @Test
//  public void test2() {
//    SqlNode node = SqlParser.create("SELECT CAST(0.0 AS double) FROM _")
//        .parseQuery();
//    System.out.println(node);
//  }
}
