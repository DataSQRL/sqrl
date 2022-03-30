package ai.dataeng.sqml.api;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.config.server.ApiVerticle;
import ai.dataeng.sqml.config.server.SourceHandler;
import ai.dataeng.sqml.config.util.StringNamedId;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.config.error.ErrorCollector;
import io.vertx.core.Vertx;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class APIServerTest {


    Environment env = null;
    DatasetRegistry registry = null;
    WebClient webClient = null;
    int port = ApiVerticle.DEFAULT_PORT;


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
            .build();
    final DataSourceUpdate dsUpdate = DataSourceUpdate.builder().name(dsName).config(fileConfig).build();
    final JsonObject fileObj = JsonObject.mapFrom(dsUpdate);
    final String deployName = "test";
    final String deployVersion = "v2";
    final ScriptBundle.Config deployConfig = ScriptBundle.Config.builder()
            .name(deployName)
            .scripts(List.of(SqrlScript.Config.builder()
                            .name(deployName)
                            .content("IMPORT data.book;\nIMPORT data.person;\n")
                            .filename(deployName+".sqrl")
                            .inputSchema("")
                            .main(true)
                            .build()))
            .version(deployVersion)
            .build();
    final JsonObject deploymentObj = JsonObject.mapFrom(deployConfig);

    public static JsonObject sourceToJson(String name, DataSourceConfiguration config) {
        JsonObject res = new JsonObject();
        res.put("name",name);
        String sourceType;
        if (config instanceof FileSourceConfiguration) sourceType = "file";
        else throw new UnsupportedOperationException();
        res.put("sourceType",sourceType);
        res.put("config",JsonObject.mapFrom(config));
        return res;
    }

    @Test
    public void testAddingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Checkpoint deploymentCheckpoint = testContext.checkpoint();
        Checkpoint requestCheckpoint = testContext.checkpoint(1);

        assertEquals(0,registry.getDatasets().size());

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            deploymentCheckpoint.flag();

            webClient.post(port, "localhost", "/source")
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(fileObj, testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonObject fileRes = resp.body();
                            assertEquals("file",fileRes.getJsonObject("config").getString("sourceType"));
                            assertEquals(dsName,fileRes.getString("name"));
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
    public void testReadDeployment(Vertx vertx, VertxTestContext testContext) throws Throwable {
        ErrorCollector errors = ErrorCollector.root();
        ScriptDeployment.Result result = env.deployScript(deployConfig,errors);
        assertNotNull(result);


        Checkpoint requestCheckpoint = testContext.checkpoint(1);
        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            webClient.get(port, "localhost", "/deployment")
                    .as(BodyCodec.jsonArray())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonArray arr = resp.body();
                            assertEquals(1, arr.size());
                            JsonObject fileRes = arr.getJsonObject(0);
                            assertEquals(deployName,fileRes.getString("name"));
                            assertEquals(deployVersion,fileRes.getString("version"));
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
    public void testAddingDeployment(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Checkpoint requestCheckpoint = testContext.checkpoint(1);
        assertEquals(0,env.getActiveDeployments().size());

        AtomicReference<String> submissionId = new AtomicReference<>("");

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {

            webClient.post(port, "localhost", "/deployment")
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(deploymentObj, testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonObject fileRes = resp.body();
                            assertEquals(deployName,fileRes.getString("name"));
                            assertEquals(deployVersion,fileRes.getString("version"));
                            String submitId = fileRes.getString("id");
                            assertNotNull(submitId);
                            submissionId.set(submitId);
                            requestCheckpoint.flag();
                        });
                    }));

        }));

        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }

        assertEquals(1,env.getActiveDeployments().size());
        Optional<ScriptDeployment.Result> deploy = env.getDeployment(StringNamedId.of(submissionId.get()));
        assertTrue(deploy.isPresent());
        assertEquals(deployName,deploy.get().getName());
        assertEquals(deployVersion,deploy.get().getVersion());
    }


    @Test
    public void testGettingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
        registry.addOrUpdateSource(dsName, fileConfig,ErrorCollector.root());
        assertNotNull(registry.getDataset(dsName));

        Checkpoint requestCheckpoint = testContext.checkpoint(3);

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            webClient.get(port, "localhost", "/source")
                    .as(BodyCodec.jsonArray())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonArray arr = resp.body();
                            assertEquals(1, arr.size());
                            JsonObject fileRes = arr.getJsonObject(0);
                            assertEquals("file",fileRes.getJsonObject("config").getString("sourceType"));
                            assertEquals(dsName,fileRes.getString("name"));
                            requestCheckpoint.flag();
                        });
                    }));

            webClient.get(port, "localhost", "/source/"+dsName)
                    .as(BodyCodec.jsonObject())
                    .send(testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(200, resp.statusCode());
                            JsonObject fileRes = resp.body();
                            assertEquals("file",fileRes.getJsonObject("config").getString("sourceType"));
                            assertEquals(dsName,fileRes.getString("name"));
                            JsonArray tables = fileRes.getJsonArray("tables");
                            assertEquals(2, tables.size());
                            requestCheckpoint.flag();
                        });
                    }));

            webClient.post(port, "localhost", "/source")
                    .as(BodyCodec.jsonArray())
                    .sendJsonObject(fileObj, testContext.succeeding(resp -> {
                        testContext.verify(() -> {
                            assertEquals(405, resp.statusCode());
                            JsonArray error = resp.body();
                            assertEquals(1, error.size());
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

//    @Test
    /**
     * This runs the server for external testing from command line. Do not include in normal
     * test suite since this test runs for a long time.
     */
    public void runServer(Vertx vertx, VertxTestContext testContext) throws Throwable {
//        registry.addOrUpdateSource(fileConfig,new ProcessMessage.ProcessBundle<>());
//        assertNotNull(registry.getDataset(dsName));

        vertx.deployVerticle(new ApiVerticle(env), testContext.succeeding(id -> {
            System.out.println("Ready to accept requests");
        }));

        testContext.awaitCompletion(120, TimeUnit.SECONDS);
    }

}
