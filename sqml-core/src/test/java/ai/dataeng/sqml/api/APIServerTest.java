package ai.dataeng.sqml.api;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.config.GlobalConfiguration;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.server.ApiVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class APIServerTest {

    @BeforeEach
    public void deleteDatabase() throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    }

    @Test
    public void testConfigFromFile(Vertx vertx, VertxTestContext testContext) {
        SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);
        Environment env = Environment.create(settings);

        WebClient webClient = WebClient.create(vertx);
        Checkpoint deploymentCheckpoint = testContext.checkpoint();
        Checkpoint requestCheckpoint = testContext.checkpoint();

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            deploymentCheckpoint.flag();

            webClient.get(8080, "localhost", "/source")
                    .as(BodyCodec.jsonArray())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            System.out.println("Body: " + resp.body());
                            JsonArray arr = resp.bodyAsJsonArray();
                            assertEquals(0, arr.size());
                            requestCheckpoint.flag();
                        });
                    }));

        }));

    }

}
