/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.Compiler;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final Path DEFAULT_DEPLOY_DIR = Path.of("build", "deploy");
  protected final boolean execute;
  protected final boolean startGraphql;

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) API specification")
  protected Path[] files;

  @CommandLine.Option(names = {"-a", "--api"}, description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(names = {"-d", "--debug"}, description = "Outputs table changestream to configured sink for debugging")
  protected boolean debug = false;

  @CommandLine.Option(names = {"-t", "--target"}, description = "Target directory for deployment artifacts")
  protected Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {"--nolookup"}, description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;
  @CommandLine.Option(names = {"-k", "--kafka"}, description = "Force start a local embedded kafka",
      scope = ScopeType.INHERIT)
  protected boolean forceStartKafka = false;
  private boolean kafkaStarted = false;

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql, boolean startKafka) {
    this.execute = execute;
    this.startGraphql = startGraphql;
    this.startKafka = startKafka;
  }

  public void runCommand(ErrorCollector errors) {
    if (DEFAULT_DEPLOY_DIR.equals(targetDir)) {
      targetDir = root.rootDir.resolve(targetDir);
    }

    if (forceStartKafka) {
      startKafka();
    }
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors, getDefaultConfig(startKafka, root.rootDir, errors));

    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);

    Build build = new Build(errors);
    Path packageFilePath = build.build(packager, !noinfer);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);
    DatabaseEngine dbEngine = pipelineFactory.getDatabaseEngine();
    errors.checkFatal(dbEngine instanceof JDBCEngine, "Expected configured "
        + "database engine to be a JDBC database: %s");
    JdbcDataSystemConnector jdbc = ((JDBCEngine)dbEngine).getConnector();
    createGraphqlKafkaSources(root.rootDir, packageFilePath.getParent(), config);
    Compiler compiler = new Compiler();

    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));
    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug, targetDir);

    addDockerCompose();
    if (isGenerateGraphql()) {
      addGraphql(packager.getBuildDir(), packager.getRootDir());
    }

    if (execute) {
      executePlan(result.getPlan(), errors);
    }

    if (errors.isFatal()) {
      throw new RuntimeException("Could not run");
    }
  }

  private boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  private void addGraphql(Path build, Path rootDir) {
    Files.copy(build.resolve(GRAPHQL_NORMALIZED_FILE_NAME),
        rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME));
  }

  //Adds in regardless
  private void addDockerCompose() {
    //Copy in docker-compose.yaml
    URL url = Resources.getResource("docker-compose.yml");
    Path toFile = targetDir.resolve("docker-compose.yml");
    try {
      Files.createDirectories(targetDir);
      Files.copy(Path.of(url.toURI()), toFile);
    } catch (Exception e) {
      log.error("Could not copy docker-compose file.");
      throw new RuntimeException(e);
    }
  }

  private Supplier<SqrlConfig> getDefaultConfig(boolean startKafka, Path rootDir, ErrorCollector errors) {
    if (startKafka) {
      startKafka();
      return ()->PackagerUtil.createEmbeddedConfig(root.rootDir, errors);
    }

    //Generate docker config
    return ()->PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
  }

  private void startKafka() {
    //We're generating an embedded config, start the cluster
    try {
      if (!kafkaStarted) {
        CLUSTER.start();
      }
      this.kafkaStarted = true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  private void createGraphqlKafkaSources(Path rootDir, Path buildDir, SqrlConfig config) {
    ResourceResolver resourceResolver = new FileResourceResolver(buildDir);

    Map<String, Optional<String>> scriptFiles = ScriptConfiguration.getFiles(config);
    Optional<URI> graphqlSchema = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
            .flatMap(file -> resourceResolver.resolveFile(NamePath.of(file)));

    if (graphqlSchema.isEmpty()) {
      return;
    }
    Path schemaPath = Paths.get(graphqlSchema.get());
    String schemaName = schemaPath.getFileName().toString().split("\\.")[0];

    String schema = Files.readString(schemaPath);
    TypeDefinitionRegistry registry = new SchemaParser().parse(schema);

    ObjectTypeDefinition mutationType = (ObjectTypeDefinition) registry
            .getType("Mutation")
            .orElse(null);
    if (mutationType == null) {
      return;
    }

    Path schemaRoot = buildDir.resolve(schemaName);
    Files.createDirectories(schemaRoot);
    for (graphql.language.FieldDefinition definition : mutationType.getFieldDefinitions()) {
      String bootstrapServers;
      if (kafkaStarted) {
        CLUSTER.createTopic(definition.getName());
        bootstrapServers = CLUSTER.bootstrapServers();
      } else {
        bootstrapServers = "kafka:9092";
      }
      Map sourceConfig = createSourceConfig(definition.getName(), bootstrapServers);

      //Check local dir to not overwrite
      String tableName = definition.getName() + ".table.json";

      if (!Files.exists(rootDir.resolve(schemaName).resolve(tableName))) {
        Path f = schemaRoot.resolve(tableName);
        SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter()
                .writeValue(f.toFile(), sourceConfig);
      }
    }
  }


  private Map createSourceConfig(String tableName, String bootstrapServers) {
    Map connector = new HashMap();
    connector.put("name", "kafka");
    connector.put("bootstrap.servers", bootstrapServers);
    connector.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    connector.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    connector.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-topic-event-lister");
    connector.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    connector.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    connector.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    connector.put("topic", tableName);

    Map config = Map.of("type", ExternalDataType.source_and_sink.name(),
            "canonicalizer", "system",
            "format", Map.of(
                    "name", "json"
            ),
            "identifier", tableName,
            "schema", "flexible",
            "connector", connector
    );

    return config;
  }

  private void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    Predicate<ExecutionStage> stageFilter = s -> true;
    if (!startGraphql) stageFilter = s -> s.getEngine().getType()!= Type.SERVER;
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
    result.get();
  }
}
