package ai.datasqrl;


import ai.datasqrl.compile.Compiler;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import javax.swing.text.html.Option;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class C360BundleTest {

  String c360Script = "IMPORT ecommerce-data.Customer;\n"
      + "IMPORT ecommerce-data.Product;\n"
      + "IMPORT ecommerce-data.Orders;"
      + "Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n"
      + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
      + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
      + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
      + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
      + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
//
//      //Error adding a nested column
      + "Orders.entries.discount2 := COALESCE(discount, 0.0)\n;"
//      + "Orders.x := EPOCH_TO_TIMESTAMP(100);"
//      //State table error
//      + "Orders.stats := SELECT "
//      + "                        SUM(quantity * unit_price - discount2) AS total, "
//      + "                        SUM(quantity * discount2) AS total_savings, "
//      + "                        COUNT(1) AS total_entries "
//      + "                 FROM _.entries e;\n"
      + "";
  @Test
  @SneakyThrows
  public void testByoSchema() {
    Path dest = copyBundle(c360Script);
    Compiler compiler = new Compiler();
    compiler.run(dest.resolve("build/"), Optional.of(dest.resolve("schema.graphqls")));

// Uncomment to test graphql
//    while(true) {
//      Thread.sleep(10);
//    }
  }

  @Test
  @SneakyThrows
  public void test() {
    Path dest = copyBundle(c360Script);
    Compiler compiler = new Compiler();
    compiler.run(dest.resolve("build/"), Optional.empty());

    HttpResponse<String> s = compiler.testQuery("{\n"
        + "  Orders {\n"
        + "    id\n"
        + "    entries(productid: 1332) {\n"
        + "      productid\n"
        + "      discount\n"
        + "    }\n"
        + "  }\n"
        + "  Customer {\n"
        + "    customerid\n"
        + "    orders {\n"
        + "      customerid\n"
        + "    }\n"
        + "  }\n"
        + "  Product(productid: 1332) {\n"
        + "    \n"
        + "    description\n"
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
