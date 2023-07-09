package com.datasqrl.graphql;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestClient;
import com.datasqrl.util.TestCompiler;
import com.datasqrl.util.TestExecutor;
import com.datasqrl.util.data.UseCaseExample;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import lombok.SneakyThrows;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.datasqrl.util.TestPackager.createPackageOverride;

@Slf4j
@Testcontainers
@ExtendWith(VertxExtension.class)
public abstract class AbstractGraphqlTest {

  @Container
  protected final PostgreSQLContainer testDatabase = new PostgreSQLContainer(
      DockerImageName.parse("postgres:14.2")).withDatabaseName("foo").withUsername("foo")
      .withPassword("secret").withDatabaseName("datasqrl");

  public static final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  protected SnapshotTest.Snapshot snapshot;
  protected Vertx vertx;
  protected Path packageOverride;
  protected TestCompiler compiler;
  protected TestExecutor executor;
  protected TestClient client;

  @BeforeEach
  public void setup(TestInfo testInfo, Vertx vertx) throws IOException {
    kafka.start();
    log.info("Kafka started: " + kafka.getAllTopicsInCluster());
    packageOverride = createPackageOverride(kafka, testDatabase);

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    this.vertx = vertx;
    this.compiler = new TestCompiler();
    this.executor = new TestExecutor(vertx);
    this.client = new TestClient(vertx);
  }

  @SneakyThrows
  @AfterEach
  public void tearDown() {
    testDatabase.stop();
    kafka.stop();
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
    return executor.executePipeline(
        compiler.compile(
        example.getRootPackageDirectory(),
        packageOverride,
        example.getScripts().get(0).getScriptPath(),
        example.getGraphqlSchemaPath()));
  }
}
