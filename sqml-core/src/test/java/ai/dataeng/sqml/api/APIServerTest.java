package ai.dataeng.sqml.api;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.config.GlobalConfiguration;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.server.ApiVerticle;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.type.basic.ProcessMessage;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class APIServerTest {


    Environment env = null;
    DatasetRegistry registry = null;
    WebClient webClient = null;


    @BeforeEach
    public void setup(Vertx vertx) throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
        SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);
        env = Environment.create(settings);
        registry = env.getDatasetRegistry();
        webClient = WebClient.create(vertx);
    }

    @AfterEach
    public void close() {
        env.close();
        env = null;
        registry = null;
        webClient.close();
        webClient = null;
    }

    final String dsName = "bookclub";
    final FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
            .uri(ConfigurationTest.DATA_DIR.toAbsolutePath().toString())
            .name(dsName)
            .build();
    final JsonObject fileObj = JsonObject.mapFrom(fileConfig);


    @Test
    public void testAddingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Checkpoint deploymentCheckpoint = testContext.checkpoint();
        Checkpoint requestCheckpoint = testContext.checkpoint(1);

        assertEquals(0,registry.getDatasets().size());

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            deploymentCheckpoint.flag();

            webClient.post(8080, "localhost", "/source/file")
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(fileObj, testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonObject fileRes = resp.body();
                            assertEquals("FileSourceConfig",fileRes.getString("objectType"));
                            assertEquals(dsName,fileRes.getString("sourceName"));
                            requestCheckpoint.flag();
                        });
                    }));

        }));

        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }

        assertEquals(1,registry.getDatasets().size());
        SourceDataset ds = registry.getDataset(dsName);
        assertNotNull(ds);
        assertEquals(dsName,ds.getName().getCanonical());

    }

    @Test
    public void testSubmissions(Vertx vertx, VertxTestContext testContext) throws Throwable {

        Checkpoint requestCheckpoint = testContext.checkpoint(1);
        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            webClient.get(8080, "localhost", "/submission")
                    .as(BodyCodec.jsonArray())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonArray arr = resp.body();
                            assertEquals(0, arr.size());
                            requestCheckpoint.flag();
                        });
                    }));
        }));

        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
    }

    @Test
    public void testGettingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
        registry.addOrUpdateSource(fileConfig,new ProcessMessage.ProcessBundle<>());
        assertNotNull(registry.getDataset(dsName));

        Checkpoint requestCheckpoint = testContext.checkpoint(3);

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            webClient.get(8080, "localhost", "/source")
                    .as(BodyCodec.jsonArray())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonArray arr = resp.body();
                            assertEquals(1, arr.size());
                            JsonObject fileRes = arr.getJsonObject(0);
                            assertEquals("FileSourceConfig",fileRes.getString("objectType"));
                            assertEquals(dsName,fileRes.getString("sourceName"));
                            requestCheckpoint.flag();
                        });
                    }));

            webClient.get(8080, "localhost", "/source/"+dsName)
                    .as(BodyCodec.jsonObject())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonObject fileRes = resp.body();
                            assertEquals("FileSourceConfig",fileRes.getString("objectType"));
                            assertEquals(dsName,fileRes.getString("sourceName"));
                            requestCheckpoint.flag();
                        });
                    }));

            webClient.post(8080, "localhost", "/source/file")
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(fileObj, testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(405, resp.statusCode());
                            JsonObject error = resp.body();
                            assertEquals(405, error.getInteger("code"));
//                            System.out.println("Error msg: " + error.getString("message"));
                            requestCheckpoint.flag();
                        });
                    }));


        }));

        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }

        DatasetRegistry registry = env.getDatasetRegistry();
        assertEquals(1,registry.getDatasets().size());
    }

    @Test
    public void testDeleteSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
        //TODO: implement
        testContext.completeNow();
    }

}
