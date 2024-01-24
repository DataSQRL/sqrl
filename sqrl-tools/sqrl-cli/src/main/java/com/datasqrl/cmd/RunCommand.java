/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;

import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.EngineKeys;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfig.Value;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.server.GenericJavaServerEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.packager.Packager;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
public class RunCommand extends AbstractCompilerCommand {
  EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

  public RunCommand() {
    super(CompileTarget.RUN);
  }

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    //start services, if there is no package json, create one and add it,
    CLUSTER.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> CLUSTER.stop()));
    SqrlConfig sqrlConfig = initializeConfig(root.rootDir, errors);
    Path config = Packager.writeEngineConfig(root.rootDir, sqrlConfig);

    this.root.packageFiles = new ArrayList<>(this.root.packageFiles);
    this.root.packageFiles.add(config);

    super.execute(errors);
  }

  @Override
  protected void postprocess(Packager packager, CompilerResult result, Path targetDir,
      ErrorCollector errors) {
    super.postprocess(packager, result, targetDir, errors);
    executePlan(result.getPlan(), errors);

  }
  @SneakyThrows
  protected void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    Predicate<ExecutionStage> stageFilter = s -> true;
//    if (!startGraphql) stageFilter = s -> s.getEngine().getType()!= Type.SERVER;
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
    result.get().get();

    // Hold java open if service is not long running
    System.in.read();
  }

  @SneakyThrows
  protected SqrlConfig initializeConfig(Path rootDir, ErrorCollector errors) {
    SqrlConfig userConfig = Packager.findPackageFile(rootDir, this.root.packageFiles)
        .map(p -> SqrlConfigCommons.fromFiles(errors, p))
        .orElseGet(() -> createLocalConfig(CLUSTER.bootstrapServers(), errors));

    // Overwrite the bootstrap servers since they change per invocation
    Value<String> bootstrapServers = userConfig
        .getSubConfig(EngineKeys.ENGINES)
        .getSubConfig(EngineKeys.LOG)
        .getSubConfig(CONNECTOR_KEY)
        .asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

    if (bootstrapServers.getOptional().isEmpty()) {
      userConfig
          .getSubConfig(EngineKeys.ENGINES)
          .getSubConfig(EngineKeys.LOG)
          .getSubConfig(CONNECTOR_KEY)
          .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    }

    return userConfig;
  }

  public static SqrlConfig createLocalConfig(String bootstrapServers, ErrorCollector errors) {
    SqrlConfig rootConfig = SqrlConfigCommons.create(errors);

    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);

    SqrlConfig dbConfig = config.getSubConfig(EngineKeys.DATABASE);
    dbConfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
    dbConfig.setProperties(JdbcDataSystemConnector.builder()
        .url("jdbc:postgresql://localhost/datasqrl")
        .driver("org.postgresql.Driver")
        .dialect("postgres")
        .database("datasqrl")
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .build()
    );

    SqrlConfig flinkConfig = config.getSubConfig(EngineKeys.STREAMS);
    flinkConfig.setProperty(FlinkEngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);
    flinkConfig.setProperty(ConfigConstants.LOCAL_START_WEBSERVER, "true");
    flinkConfig.setProperty(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), "256mb");
    flinkConfig.setProperty(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "256mb");
    flinkConfig.setProperty(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "256mb");

    SqrlConfig server = config.getSubConfig(EngineKeys.SERVER);
    server.setProperty(GenericJavaServerEngineFactory.ENGINE_NAME_KEY,
        VertxEngineFactory.ENGINE_NAME);

    SqrlConfig logConfig = config.getSubConfig(EngineKeys.LOG);
    logConfig.setProperty(EngineFactory.ENGINE_NAME_KEY, KafkaLogEngineFactory.ENGINE_NAME);
    logConfig.copy(
        KafkaDataSystemFactory.getKafkaEngineConfig(KafkaLogEngineFactory.ENGINE_NAME, bootstrapServers,
            JsonLineFormat.NAME, FlexibleTableSchemaFactory.SCHEMA_TYPE));

    return rootConfig;
  }
}
