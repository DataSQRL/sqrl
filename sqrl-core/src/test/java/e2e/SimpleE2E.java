package e2e;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.api.InputModel;
import ai.datasqrl.api.InputModel.DataSource;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.config.server.ApiVerticle;
import ai.datasqrl.util.JDBCTestDatabase;
import ai.datasqrl.util.data.C360;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.sql.Connection;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Disabled
@ExtendWith(VertxExtension.class)
public class SimpleE2E extends AbstractSQRLIT {
  WebClient webClient;

  @SneakyThrows
  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    initialize(IntegrationTestSettings.builder().monitorSources(false)
        .stream(IntegrationTestSettings.StreamEngine.FLINK)
        .database(IntegrationTestSettings.DatabaseEngine.POSTGRES).build());

    JDBCTestDatabase jdbc = (JDBCTestDatabase)this.database;
    Connection con = jdbc.getJdbcConfiguration()
        .getDatabase("postgres")
        .getConnection();
    con.createStatement()
        .execute("CREATE DATABASE test$v1");
    con.close();

    vertx.deployVerticle(new ApiVerticle(env), testContext.succeedingThenComplete());
    webClient = WebClient.create(vertx);
  }

  @Test
  public void test(VertxTestContext testContext) {
    Future<HttpResponse<JsonObject>> step1 = this.registerSource(
        new DataSource(C360.BASIC.getName(), C360.BASIC.getSource())
    );
    step1.onFailure(testContext::failNow);

    ScriptBundle.Config conf = ScriptBundle.Config.builder()
        .name("test")
        .version("")
        .scripts(List.of(SqrlScript.Config.builder()
                .content("IMPORT ecommerce-data.Orders;")
                .filename("c360.sqrl")
                .name("c360")
                .main(true)
            .build()))
        .build();
    Future<HttpResponse<JsonObject>> step2 = step1.compose(o->this.submitScript(conf));
    step2.onFailure(testContext::failNow);

    step2.andThen(testContext.succeedingThenComplete());
  }

  private Future<HttpResponse<JsonObject>> submitScript(ScriptBundle.Config conf) {
    return webClient.post(ApiVerticle.DEFAULT_PORT, "localhost", "/deployment")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(JsonObject.mapFrom(conf))
        .onFailure(e -> e.printStackTrace());
  }

  private Future<HttpResponse<JsonObject>> registerSource(InputModel.DataSource source) {
    return webClient.post(ApiVerticle.DEFAULT_PORT, "localhost", "/source")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(JsonObject.mapFrom(source))
        .onFailure(e->e.printStackTrace());
  }
}
