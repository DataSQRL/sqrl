package com.datasqrl;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.config.provider.Dialect;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.packager.Packager;
import com.datasqrl.physical.PhysicalPlan;
import com.datasqrl.physical.PhysicalPlanExecutor;
import com.datasqrl.physical.database.relational.JDBCEngineConfiguration;
import com.datasqrl.physical.stream.flink.FlinkEngineConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class Demo implements Runnable {

  @Option(names = {"-b", "--build-dir"}, description = "Base directory", defaultValue = ".")
  private Path buildDir = Path.of(".");

  @Option(names = {"-m", "--main-script"}, description = "Main script", defaultValue = "main.sqrl")
  private String mainScript = "main.sqrl";

  @Option(names = {"-p", "--package-file"}, description = "Package file", defaultValue = "package.json")
  private String baseConfig = "package.json";

  @Option(names = {"-g", "--generate-schema"} ,description = "Generates the graphql "
      + "schema file and exits")
  private boolean generateSchema = false;

  @Option(names = {"-h", "--help"}, usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested = false;

  public static void main(String[] args) {
    new CommandLine(new Demo()).execute(args);
  }

  @SneakyThrows
  public void run() {
    ErrorCollector collector = ErrorCollector.root();

    try {
      deploy(collector);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(ErrorPrinter.prettyPrint(collector));

  }
  @SneakyThrows
  public void deploy(ErrorCollector collector) {
    //We must start postgres immediately to get its mapped ports
    PostgreSQLContainer postgreSQLContainer = startPostgres();

    Path engineConfig = writeEngineConfig(postgreSQLContainer);

    Optional<Path> graphqlSchema = getSchemaFile();
    Packager packager = buildPackager(engineConfig, graphqlSchema);

    packager.cleanUp();
    packager.inferDependencies();
    packager.populateBuildDir();

    //compile
    boolean writeSchema = graphqlSchema.map(s->false)
        .orElse(generateSchema);
    Compiler compiler = new Compiler(writeSchema);

    Path buildPath = buildDir
        .resolve("build")
        .resolve("package.json");

    CompilerResult result = compiler.run(collector, buildPath);

    if (generateSchema) {
      log.info("Wrote schema. Exiting.");
      return;
    }

    //execute flink
    executePlan(result.getPlan());

    //execute graphql server
    startGraphQLServer(result.getModel(), postgreSQLContainer);
  }

  @SneakyThrows
  private Packager buildPackager(Path engineConfig, Optional<Path> graphqlSchema) {
    Packager.Config.ConfigBuilder builder = Packager.Config.builder();
    Path main = buildDir.resolve(mainScript);
    if (!main.toFile().exists()) {
      throw new RuntimeException("Could not find main script: " + mainScript);
    }

    builder.mainScript(main);

    Path basePath = buildDir.resolve(this.baseConfig);
    if (!basePath.toFile().exists()) {
      throw new RuntimeException("Could not find package conf: " + basePath);
    }

    builder.packageFiles(List.of(basePath, engineConfig));


    graphqlSchema.ifPresent(builder::graphQLSchemaFile);

    Packager pkger = builder.build().getPackager();
    return pkger;
  }

  @SneakyThrows
  private Path writeEngineConfig(PostgreSQLContainer postgreSQLContainer) {
    JDBCEngineConfiguration jdbcEngineConfiguration =
        JDBCEngineConfiguration.builder()
            .dbURL(postgreSQLContainer.getJdbcUrl())
            .host(postgreSQLContainer.getHost())
            .user(postgreSQLContainer.getUsername())
            .port(postgreSQLContainer.getMappedPort(5432))
            .dialect(Dialect.POSTGRES)
            .password(postgreSQLContainer.getPassword())
            .database(postgreSQLContainer.getDatabaseName())
            .driverName(postgreSQLContainer.getDriverClassName())
            .build();

    FlinkEngineConfiguration flinkEngineConfiguration =
        FlinkEngineConfiguration.builder()
            .savepoint(false)
            .build();

    Path enginesFile = Files.createTempFile("engines", ".json");
    File file = enginesFile.toFile();
    file.deleteOnExit();

    ObjectMapper mapper = new ObjectMapper();
    String enginesConf = mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(Map.of("engines",
            List.of(flinkEngineConfiguration
                , jdbcEngineConfiguration
            )));

    Files.write(enginesFile, enginesConf.getBytes(StandardCharsets.UTF_8));

    return enginesFile;
  }

  private PostgreSQLContainer startPostgres() {
    DockerImageName image = DockerImageName.parse("postgres:14.2");
    PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer(image)
        .withDatabaseName("datasqrl");
    postgreSQLContainer.start();
    return postgreSQLContainer;
  }

  private Optional<Path> getSchemaFile() {
    for(File file : buildDir.toFile().listFiles()) {
      if (file.getName().endsWith(".graphqls")) {
        log.info("Found schema {}", file.toPath());
        return Optional.of(file.toPath());
      }
    }

    return Optional.empty();
  }

  private void executePlan(PhysicalPlan physicalPlan) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan);
  }

  @SneakyThrows
  private void startGraphQLServer(RootGraphqlModel model, PostgreSQLContainer postgreSQLContainer) {
    CompletableFuture future = Vertx.vertx().deployVerticle(new GraphQLServer(
            model, toPgOptions(postgreSQLContainer), 8888, new PoolOptions()))
        .toCompletionStage()
        .toCompletableFuture();
    log.info("Server started at: http://localhost:8888/graphiql/");

    future.get();
  }

  private PgConnectOptions toPgOptions(PostgreSQLContainer jdbcConf) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(jdbcConf.getDatabaseName());
    options.setHost(jdbcConf.getHost());
    options.setPort(jdbcConf.getMappedPort(5432));
    options.setUser(jdbcConf.getUsername());
    options.setPassword(jdbcConf.getPassword());
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    return options;
  }
}