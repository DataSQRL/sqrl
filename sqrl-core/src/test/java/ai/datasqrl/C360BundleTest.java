package ai.datasqrl;


import ai.datasqrl.compile.Compiler;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class C360BundleTest {

  String c360Script = "IMPORT ecommerce-data.Customer;\n"
      + "IMPORT ecommerce-data.Product;\n"
      + "IMPORT ecommerce-data.Orders;"
      + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
      + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
      + "Customer.orders := JOIN _ INNER JOIN Orders ON Orders.customerid = _.customerid;\n"
      + "Orders.entries.product := JOIN _ INNER JOIN Product ON Product.productid = _.productid;\n"
      + "Product.order_entries := JOIN _ INNER JOIN Orders.entries e ON e.productid = _.productid;\n"

      //Error adding a nested column
//      + "Orders.entries.discount := COALESCE(discount, 0.0)\n;"
      //State table error
      + "Orders.stats := SELECT _._uuid AS _uuid, "
      + "                        SUM(quantity * unit_price - discount) AS total, "
      + "                        SUM(discount) AS total_savings, "
      + "                        COUNT(1) AS total_entries "
      + "                 FROM _ "
      + "                 INNER JOIN _.entries e"
      + "                 GROUP BY _._uuid;\n"
      + "";

  @Test
  @SneakyThrows
  public void test() {
    Path dest = Files.createTempDirectory("c360bundle");
    dest.toFile().deleteOnExit();
    Path src = Path.of("src/test/resources/c360bundle/");

    copyDirectory(src, dest);

    Files.write(dest.resolve("build/").resolve("main.sqrl"), c360Script.getBytes(StandardCharsets.UTF_8));
    dest.resolve("build/").resolve("main.sqrl").toFile().deleteOnExit();
    Compiler compiler = new Compiler();
    compiler.run(dest.resolve("build/"));

    HttpResponse<String> s = compiler.testQuery("query {\n"
        + "  Customer {\n"
        + "    customerid\n"
        + "    orders {\n"
        + "      customerid \n"
        + "      id\n"
        + "      entries {\n"
        + "        discount\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "  Orders {\n"
        + "    id \n"
        + "  \tcustomerid\n"
        + "    entries {\n"
        + "      quantity\n"
        + "      discount\n"
        + "      product {\n"
        + "        description\n"
        + "        name\n"
        + "      }\n"
        + "      parent {\n"
        + "        id\n"
        + "\t\t\t}\n"
        + "    }\n"
        + "  }\n"
        + "  Product {\n"
        + "    description\n"
        + "    order_entries {\n"
        + "      discount\n"
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
