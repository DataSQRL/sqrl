package com.datasqrl.graphql;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.io.KafkaBaseTest;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestClient;
import com.datasqrl.util.TestCompiler;
import com.datasqrl.util.TestExecutor;
import com.datasqrl.util.data.UseCaseExample;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.datasqrl.util.TestClient.NO_HANDLER;
import static com.datasqrl.util.TestPackager.createPackageOverride;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@Testcontainers
@ExtendWith(VertxExtension.class)
public abstract class AbstractGraphqlTest extends KafkaBaseTest {
  protected List<String> events = new ArrayList<>();

  @Container
  static PostgreSQLContainer testDatabase = new PostgreSQLContainer(
      DockerImageName.parse("ankane/pgvector:v0.5.0")
      .asCompatibleSubstituteFor("postgres"));

  protected SnapshotTest.Snapshot snapshot;
  protected Vertx vertx;
  protected Path packageOverride;
  protected TestCompiler compiler;
  protected TestExecutor executor;
  protected TestClient client;

  @BeforeEach
  public void setup(TestInfo testInfo, Vertx vertx) throws IOException {
    CLUSTER.start();
    log.info("Kafka started: " + CLUSTER.getAllTopicsInCluster());
    packageOverride = createPackageOverride(CLUSTER, testDatabase);

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    this.vertx = vertx;
    this.compiler = new TestCompiler();
    this.executor = new TestExecutor(vertx);
    this.client = new TestClient(vertx);
  }

  @SneakyThrows
  @AfterEach
  public void tearDown() {
    super.tearDown();
    try {
      for (String id : vertx.deploymentIDs()) {
        vertx.undeploy(id).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      }
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
    }

    Files.deleteIfExists(packageOverride);
  }

  protected CompletableFuture<ExecutionResult> execute(String path) {
    return executor.executePipeline(
        compiler.compile(Path.of(path), packageOverride));
  }

  protected CompletableFuture<ExecutionResult> execute(UseCaseExample example) {
    return execute(example.getRootPackageDirectory(), example.getScripts().get(0).getScriptPath(),
                   example.getGraphqlSchemaPath());
  }

  protected CompletableFuture<ExecutionResult> execute(Path rootPath,
      Path scriptPath, Path graphqlPath) {
    return executor.executePipeline(
        compiler.compile(
            rootPath,
            packageOverride,
            scriptPath,
            graphqlPath));
  }


  @SneakyThrows
  protected CountDownLatch subscribeToAlert(String query) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    client.listen(query, (t) -> {
      events.add(t.toString());
      countDownLatch.countDown();
    }).future().toCompletionStage().toCompletableFuture().get();
    return countDownLatch;
  }

  protected void executeMutation(String query, JsonObject input) {
    executeMutation(query, input, NO_HANDLER);
  }

  protected void executeMutation(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback) {
    client.query(query, input, callback);
  }

  protected void executeQuery(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback) {
    client.query(query, input, callback);
  }

  protected void executePersistedQuery(String querySha, JsonObject input, Consumer<HttpResponse<JsonObject>> callback) {
    client.executePersistedQuery(querySha, input, callback);
  }

  @FunctionalInterface
  public interface ExecuteQuery {
    public void executePersistedQuery(String querySha, JsonObject input,
        Consumer<HttpResponse<JsonObject>> callback);
  }

  public void executeQueryUntilTrue(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback,
      Predicate<HttpResponse<JsonObject>> waitUntilTrue) {
    executeQueryUntilTrue(query, input, callback, waitUntilTrue, 30);
  }

  @SneakyThrows
  public void executeQueryUntilTrue(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback,
      Predicate<HttpResponse<JsonObject>> waitUntilTrue, int times) {
    executeUntilTrue(query, input, callback, waitUntilTrue, times, this::executeQuery);
  }

  @SneakyThrows
  public void executePreparsedQueryUntilTrue(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback,
      Predicate<HttpResponse<JsonObject>> waitUntilTrue, int times) {
    executeUntilTrue(query, input, callback, waitUntilTrue, times, this::executePersistedQuery);
  }

  @SneakyThrows
  public void executeUntilTrue(String query, JsonObject input, Consumer<HttpResponse<JsonObject>> callback,
      Predicate<HttpResponse<JsonObject>> waitUntilTrue, int times, ExecuteQuery executeQuery) {
    AtomicBoolean done = new AtomicBoolean(false);
    while (!done.get() && times-- > 0) {
      executeQuery.executePersistedQuery(query, input, t-> {
        if (waitUntilTrue.test(t)) {
          done.set(true);
          callback.accept(t);
        }
      });

      Thread.sleep(1000);
    }
    if (!done.get()) {
      fail("Query yielded no results");
    }
  }

  protected void validateEvents() {
    Collections.sort(events);
    snapshot.addContent(String.join("\n", events))
        .createOrValidate();
    snapshot.createOrValidate();
  }
}
