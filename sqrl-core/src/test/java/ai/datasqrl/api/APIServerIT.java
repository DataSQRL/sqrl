package ai.datasqrl.api;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.server.HandlerUtil;
import ai.datasqrl.config.server.SourceHandler;
import ai.datasqrl.environment.ScriptDeployment;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.server.ApiVerticle;
import ai.datasqrl.config.util.StringNamedId;
import ai.datasqrl.io.formats.JsonLineFormat;
import ai.datasqrl.io.impl.file.DirectorySinkImplementation;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.sinks.DataSink;
import ai.datasqrl.io.sinks.DataSinkConfiguration;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.io.sources.DataSourceImplementation;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.util.TestResources;
import ai.datasqrl.util.data.BookClub;
import com.google.common.collect.ImmutableSet;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import ai.datasqrl.config.server.*;

@ExtendWith(VertxExtension.class)
public class APIServerIT extends AbstractSQRLIT {

  int port = ApiVerticle.DEFAULT_PORT;

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    initialize(IntegrationTestSettings.getInMemory(false));

    //String JSONResponse = "[{\"sources\":[\"testSource1\",\"testSource2\"]}]";

    String JSONErrorOpenAPI = "[{\"message\":\"Column `id` ambiguous\","
                              + "\"severity\":\"fatal\","
                              + "\"location\":{"
                              + "\"prefix\":\"file://\","
                              + "\"path\":\"example/example.sqrl\","
                              + "\"file\":{"
                              + "\"line\":10,"
                              + "\"offset\":42"
                              + "}}}]";

    String JSONErrorExtended = "[{\"message\":\"Column `id` ambiguous\","
                              + "\"severity\":\"fatal\","
                              + "\"location\":{"
                              + "\"prefix\":\"file://\","
                              + "\"path\":\"example/example.sqrl\","
                              + "\"file\":{"
                              + "\"line\":10,"
                              + "\"offset\":7,"
                              + "\"context\": {"
                              + "\"text\": \"SELECT id, lastName, price, quantity, sku FROM customers c JOIN orders o ON c.orderNo = o.orderNo\","
                              + "\"highlight_start\": 7,"
                              + "\"highlight_end\": 9"
                              + "}}}}]";

    System.out.println("Mocked JSON response:\n"
        + JSONErrorExtended);

    SourceHandler sourceHandler = mock(SourceHandler.class);
    when(sourceHandler.get()).thenReturn(routingContext -> {
      routingContext
          .response()
          .setStatusCode(200)
          .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8")
          .end(JSONErrorExtended);
    });

    vertx.deployVerticle(new ApiVerticle(env, sourceHandler),  testContext.succeeding(id -> {
      System.out.println("Ready to accept requests");
    }));
  }

  final BookClub bookClub = BookClub.INSTANCE;
  final DirectorySourceImplementation fileConfig = bookClub.getSource();
  final String dsName = bookClub.getName();
  final JsonObject fileObj = getDataSourcePayload(dsName, fileConfig);

  public static JsonObject getDataSourcePayload(String name, DataSourceImplementation source) {
    JsonObject res = new JsonObject();
    res.put("name", name);
    res.put("source", JsonObject.mapFrom(source));
    return res;
  }

    /*
    ######## Source endpoints
     */

  @Test
  public void testAddingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    //System.out.println(fileObj);
    WebClient webClient = WebClient.create(vertx);
    assertEquals(0, sourceRegistry.getDatasets().size());

    webClient.post(port, "localhost", "/source")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(fileObj, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject fileRes = resp.body();
            assertEquals("dir", fileRes.getJsonObject("source").getString("sourceType"));
            assertEquals(dsName, fileRes.getString("name"));
            requestCheckpoint.flag();
          });
        }));

    testContext.succeedingThenComplete();

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    assertEquals(1, sourceRegistry.getDatasets().size());
    SourceDataset ds = sourceRegistry.getDataset(dsName);
    assertNotNull(ds);
    Assertions.assertEquals(dsName, ds.getName().getCanonical());
    testContext.completeNow();
  }


  @Test
  public void testGettingSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
    bookClub.registerSource(env);
    assertNotNull(sourceRegistry.getDataset(dsName));

    Checkpoint requestCheckpoint = testContext.checkpoint(3);
    WebClient webClient = WebClient.create(vertx);

    webClient.get(port, "localhost", "/source")
        .as(BodyCodec.jsonArray())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonArray arr = resp.body();
            assertEquals(1, arr.size());
            JsonObject fileRes = arr.getJsonObject(0);
            assertEquals("dir", fileRes.getJsonObject("source").getString("sourceType"));
            assertEquals(dsName, fileRes.getString("name"));
            requestCheckpoint.flag();
          });
        }));

    webClient.get(port, "localhost", "/source/" + dsName)
        .as(BodyCodec.jsonObject())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject fileRes = resp.body();
            assertEquals("dir", fileRes.getJsonObject("source").getString("sourceType"));
            assertEquals(dsName, fileRes.getString("name"));
            JsonArray tables = fileRes.getJsonArray("tables");
            assertEquals(2, tables.size());
            requestCheckpoint.flag();
          });
        }));

    webClient.post(port, "localhost", "/source")
        .as(BodyCodec.jsonArray())
        .sendJsonObject(fileObj, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(400, resp.statusCode());
            JsonArray error = resp.body();
            assertEquals(1, error.size());
//                            System.out.println("Error msg: " + error.getString("message"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    DatasetRegistry registry = env.getDatasetRegistry();
    assertEquals(1, registry.getDatasets().size());
    testContext.completeNow();
  }

  @Test
  public void testDeleteSource(Vertx vertx, VertxTestContext testContext) throws Throwable {
    bookClub.registerSource(env);
    assertNotNull(sourceRegistry.getDataset(dsName));
    assertEquals(2, sourceRegistry.getDataset(dsName).getTables().size());
    WebClient webClient = WebClient.create(vertx);
    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    webClient.delete(port, "localhost", "/source/" + dsName)
        .as(BodyCodec.jsonObject())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject fileRes = resp.body();
            assertEquals("dir", fileRes.getJsonObject("source").getString("sourceType"));
            assertEquals(dsName, fileRes.getString("name"));
            JsonArray tables = fileRes.getJsonArray("tables");
            assertEquals(2, tables.size());
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
    assertEquals(0, sourceRegistry.getDatasets().size());
    testContext.completeNow();
  }

    /*
    ######## Source Table endpoints
     */

  @Test
  public void testAddTable(Vertx vertx, VertxTestContext testContext) throws Throwable {
    DataSourceUpdate dsUpdate = DataSourceUpdate.builder().name(dsName).source(fileConfig)
        .discoverTables(false).build();
    sourceRegistry.addOrUpdateSource(dsUpdate, ErrorCollector.root());
    assertEquals(0, sourceRegistry.getDataset(dsName).getTables().size());

    SourceTableConfiguration tableConf = SourceTableConfiguration.builder()
        .name("test").identifier("book").format(new JsonLineFormat.Configuration()).build();
    JsonObject payload = JsonObject.mapFrom(tableConf);

    Checkpoint requestCheckpoint = testContext.checkpoint(1);
    WebClient webClient = WebClient.create(vertx);

    webClient.post(port, "localhost", "/source/" + dsName + "/tables")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(payload, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject fileRes = resp.body();
            assertEquals("test", fileRes.getString("name"));
            assertEquals("book", fileRes.getString("identifier"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
    assertEquals(1, sourceRegistry.getDataset(dsName).getTables().size());
    assertEquals("book",
        sourceRegistry.getDataset(dsName).getTable("test").getConfiguration().getIdentifier());
    testContext.completeNow();
  }

  @Test
  public void testGetTable(Vertx vertx, VertxTestContext testContext) throws Throwable {
    DataSourceUpdate dsUpdate = DataSourceUpdate.builder().name(dsName).source(fileConfig)
        .discoverTables(true).build();
    sourceRegistry.addOrUpdateSource(dsUpdate, ErrorCollector.root());
    assertEquals(2, sourceRegistry.getDataset(dsName).getTables().size());

    SourceTableConfiguration tableConf = SourceTableConfiguration.builder()
        .name("book").identifier("book").format(new JsonLineFormat.Configuration()).build();
    JsonObject payload = JsonObject.mapFrom(tableConf);

    Checkpoint requestCheckpoint = testContext.checkpoint(3);
    WebClient webClient = WebClient.create(vertx);

    webClient.get(port, "localhost", "/source/" + dsName + "/tables")
        .as(BodyCodec.jsonArray())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonArray arr = resp.body();
            assertEquals(2, arr.size());
            JsonObject table = arr.getJsonObject(0);
            assertEquals(table.getString("identifier"), table.getString("name"));
            assertNotNull(table.getJsonObject("format"));
            requestCheckpoint.flag();
          });

          webClient.get(port, "localhost", "/source/" + dsName + "/tables/book")
              .as(BodyCodec.jsonObject())
              .send(testContext.succeeding(resp2 -> {
                testContext.verify(() -> {
                  assertEquals(200, resp2.statusCode());
                  JsonObject table = resp2.body();
                  assertEquals("book", table.getString("identifier"));
                  assertEquals("json", table.getJsonObject("format").getString("formatType"));
                  requestCheckpoint.flag();
                });
              }));

          webClient.post(port, "localhost", "/source/" + dsName + "/tables")
              .as(BodyCodec.jsonArray())
              .sendJsonObject(payload, testContext.succeeding(resp3 -> {
                testContext.verify(() -> {
                  assertEquals(400, resp3.statusCode());
                  JsonArray error = resp3.body();
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
    testContext.completeNow();
  }

  @Test
  public void testDeleteTable(Vertx vertx, VertxTestContext testContext) throws Throwable {
    DataSourceUpdate dsUpdate = DataSourceUpdate.builder().name(dsName).source(fileConfig)
        .discoverTables(true).build();
    sourceRegistry.addOrUpdateSource(dsUpdate, ErrorCollector.root());
    assertEquals(2, sourceRegistry.getDataset(dsName).getTables().size());
    WebClient webClient = WebClient.create(vertx);
    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    webClient.delete(port, "localhost", "/source/" + dsName + "/tables/book")
        .as(BodyCodec.jsonObject())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject table = resp.body();
            assertEquals("book", table.getString("identifier"));
            assertEquals("json", table.getJsonObject("format").getString("formatType"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
    assertEquals(ImmutableSet.of("person"), sourceRegistry.getDataset(dsName).getTables().stream()
        .map(SourceTable::getName).map(Name::getCanonical).collect(Collectors.toSet()));
    testContext.completeNow();
  }

    /*
    ######## Source endpoints
     */

  static final String sinkName = "testSink";
  static final DataSinkRegistration sinkReg = DataSinkRegistration.builder()
      .name(sinkName)
      .sink(DirectorySinkImplementation.builder().uri(BookClub.DATA_DIR.toAbsolutePath().toString())
          .build())
      .config(DataSinkConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
      .build();
  static final JsonObject sinkObj = JsonObject.mapFrom(sinkReg);

  @Test
  public void testAddingSink(Vertx vertx, VertxTestContext testContext) throws Throwable {
    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    assertEquals(0, sinkRegistry.getSinks().size());
    WebClient webClient = WebClient.create(vertx);

    webClient.post(port, "localhost", "/sink")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(sinkObj, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject sinkRes = resp.body();
            assertEquals(sinkName, sinkRes.getString("name"));
            assertEquals("dir", sinkRes.getJsonObject("sink").getString("sinkType"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    assertEquals(1, sinkRegistry.getSinks().size());
    DataSink sink = sinkRegistry.getSink(sinkName);
    assertNotNull(sink);
    Assertions.assertEquals(sinkName, sink.getName().getDisplay());
    testContext.completeNow();
  }


  @Test
  public void testGettingSink(Vertx vertx, VertxTestContext testContext) throws Throwable {
    sinkRegistry.addOrUpdateSink(sinkReg, ErrorCollector.root());
    assertNotNull(sinkRegistry.getSink(sinkName));

    Checkpoint requestCheckpoint = testContext.checkpoint(2);
    WebClient webClient = WebClient.create(vertx);

    webClient.get(port, "localhost", "/sink")
        .as(BodyCodec.jsonArray())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonArray arr = resp.body();
            assertEquals(1, arr.size());
            JsonObject sinkRes = arr.getJsonObject(0);
            assertEquals("dir", sinkRes.getJsonObject("sink").getString("sinkType"));
            assertEquals(sinkName, sinkRes.getString("name"));
            requestCheckpoint.flag();
          });

          webClient.get(port, "localhost", "/sink/" + sinkName)
              .as(BodyCodec.jsonObject())
              .send(testContext.succeeding(resp2 -> {
                testContext.verify(() -> {
                  assertEquals(200, resp2.statusCode());
                  JsonObject sinkRes = resp2.body();
                  assertEquals("dir", sinkRes.getJsonObject("sink").getString("sinkType"));
                  assertEquals(sinkName, sinkRes.getString("name"));
                  requestCheckpoint.flag();
                });
              }));


        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    assertEquals(1, sinkRegistry.getSinks().size());
    testContext.completeNow();
  }

  @Test
  public void testUpdateSink(Vertx vertx, VertxTestContext testContext) throws Throwable {
    sinkRegistry.addOrUpdateSink(sinkReg, ErrorCollector.root());
    assertEquals(BookClub.DATA_DIR.toAbsolutePath().toString(),
        ((DirectorySinkImplementation) sinkRegistry.getSink(sinkName)
            .getImplementation()).getUri());

    String newDir = TestResources.RESOURCE_DIR.toAbsolutePath().toString();
    DataSinkRegistration sinkReg2 = DataSinkRegistration.builder()
        .name(sinkName)
        .sink(DirectorySinkImplementation.builder().uri(newDir).build())
        .config(DataSinkConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
        .build();
    JsonObject sinkObj2 = JsonObject.mapFrom(sinkReg2);

    Checkpoint requestCheckpoint = testContext.checkpoint(1);
    WebClient webClient = WebClient.create(vertx);

    webClient.post(port, "localhost", "/sink")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(sinkObj2, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject sinkRes = resp.body();
            assertEquals(sinkName, sinkRes.getString("name"));
            assertEquals("dir", sinkRes.getJsonObject("sink").getString("sinkType"));
            assertEquals(newDir, sinkRes.getJsonObject("sink").getString("uri"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    assertEquals(newDir,
        ((DirectorySinkImplementation) sinkRegistry.getSink(sinkName)
            .getImplementation()).getUri());
    testContext.completeNow();
  }

  @Test
  public void testDeleteSink(Vertx vertx, VertxTestContext testContext) throws Throwable {
    sinkRegistry.addOrUpdateSink(sinkReg, ErrorCollector.root());
    assertNotNull(sinkRegistry.getSink(sinkName));
    WebClient webClient = WebClient.create(vertx);

    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    webClient.delete(port, "localhost", "/sink/" + sinkName)
        .as(BodyCodec.jsonObject())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject sinkRes = resp.body();
            assertEquals("dir", sinkRes.getJsonObject("sink").getString("sinkType"));
            assertEquals(sinkName, sinkRes.getString("name"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
    assertEquals(0, sinkRegistry.getSinks().size());
    testContext.completeNow();
  }


    /*
    ######## Deployment endpoints
     */

  final String deployName = "test";
  final String deployVersion = "v2";
  final ScriptBundle.Config deployConfig = BookClub.INSTANCE.buildBundle()
      .setName(deployName).setVersion(deployVersion).getConfig();
  final JsonObject deploymentObj = JsonObject.mapFrom(deployConfig);

  @Disabled("fix after Environment is finalized")
  @Test
  public void testAddingDeployment(Vertx vertx, VertxTestContext testContext) throws Throwable {
    bookClub.registerSource(env);
    Checkpoint requestCheckpoint = testContext.checkpoint(1);
    assertEquals(0, env.getActiveDeployments().size());

    AtomicReference<String> submissionId = new AtomicReference<>("");
    WebClient webClient = WebClient.create(vertx);

    webClient.post(port, "localhost", "/deployment")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(deploymentObj, testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonObject fileRes = resp.body();
            assertEquals(deployName, fileRes.getString("name"));
            assertEquals(deployVersion, fileRes.getString("version"));
            String submitId = fileRes.getString("id");
            assertNotNull(submitId);
            submissionId.set(submitId);
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    assertEquals(1, env.getActiveDeployments().size());
    Optional<ScriptDeployment.Result> deploy = env.getDeployment(
        StringNamedId.of(submissionId.get()));
    assertTrue(deploy.isPresent());
    assertEquals(deployName, deploy.get().getName());
    assertEquals(deployVersion, deploy.get().getVersion());
    testContext.completeNow();
  }

  @Disabled("fix after Environment is finalized")
  @Test
  public void testReadDeployment(Vertx vertx, VertxTestContext testContext) throws Throwable {
    ErrorCollector errors = ErrorCollector.root();
    ScriptDeployment.Result result = env.deployScript(deployConfig, errors);
    assertNotNull(result);
    WebClient webClient = WebClient.create(vertx);

    Checkpoint requestCheckpoint = testContext.checkpoint(1);

    webClient.get(port, "localhost", "/deployment")
        .as(BodyCodec.jsonArray())
        .send(testContext.succeeding(resp -> {
          testContext.verify(() -> {
            assertEquals(200, resp.statusCode());
            JsonArray arr = resp.body();
            assertEquals(1, arr.size());
            JsonObject fileRes = arr.getJsonObject(0);
            assertEquals(deployName, fileRes.getString("name"));
            assertEquals(deployVersion, fileRes.getString("version"));
            requestCheckpoint.flag();
          });
        }));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
    testContext.completeNow();
  }

   @Test

  /**
   * This runs the server for external testing from command line. Do not include in normal test
   * suite since this test runs for a long time.
   */
  public void runServer(Vertx vertx, VertxTestContext testContext) throws Throwable {
//        registry.addOrUpdateSource(fileConfig,new ProcessMessage.ProcessBundle<>());
//        assertNotNull(registry.getDataset(dsName));

     // can't unmarshal
     // String JSONResponse = "{\"sources\":[{\"sourceType\":\"localDir\",\"uri\":\"path/to/dir\"}]}";

//     when(sourceHandler.getSourceByName()).thenReturn(routingContext -> {
//       routingContext
//           .response()
//           .setStatusCode(201)
//           .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
//           .end("{\"testGetSBN\":true}");
//     });

     // not going to work w/o changing ApiVerticle.java
//     DeploymentHandler deployHandler = mock(DeploymentHandler.class);
//
//     when(deployHandler.deploy()).thenReturn(routingContext -> {
//       routingContext
//           .response()
//           .setStatusCode(201)
//           .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
//           .end("{ \"testDeploy\":true}");
//     });


//    vertx.deployVerticle(new ApiVerticle(env, sourceHandler), testContext.succeeding(id -> {
//      System.out.println("Ready to accept requests");
//    }));

    testContext.awaitCompletion(10000000, TimeUnit.SECONDS);
  }

}
