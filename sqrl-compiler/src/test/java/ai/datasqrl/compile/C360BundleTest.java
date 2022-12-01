package ai.datasqrl.compile;


import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class C360BundleTest extends AbstractEngineIT {

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
    initialize(IntegrationTestSettings.getFlinkWithDB());
    Path dest = copyBundle(c360Script);
    ai.datasqrl.compile.Compiler compiler = new ai.datasqrl.compile.Compiler();
    compiler.run(ErrorCollector.root(),
        dest.resolve("build/"), Optional.of(dest.resolve("schema.graphqls")), engineSettings);

    HttpResponse<String> s = testQuery("{\n"
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

    assertEquals(200, s.statusCode());
    assertEquals(s.body().length(), 726);
    assertEquals(200, s.statusCode());
//    assertEquals(s.body().length(), 726);
// Uncomment to test graphql
//    while(true) {
//      Thread.sleep(10);
//    }
  }

  @SneakyThrows
  public static HttpResponse<String> testQuery(String query) {
    ObjectMapper mapper = new ObjectMapper();
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(
            Map.of("query", query))))
        .uri(URI.create("http://localhost:8888/graphql"))
        .build();
    return client.send(request, BodyHandlers.ofString());
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
}
