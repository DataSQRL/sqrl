///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.cmd;
//
//import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;
//
//import com.datasqrl.cmd.EngineKeys;
//import com.datasqrl.config.PipelineFactory;
//import com.datasqrl.config.SqrlConfig;
//import com.datasqrl.config.SqrlConfig.Value;
//import com.datasqrl.config.SqrlConfigCommons;
//import com.datasqrl.engine.EngineFactory;
//import com.datasqrl.engine.PhysicalPlan;
//import com.datasqrl.engine.PhysicalPlanExecutor;
//import com.datasqrl.engine.database.relational.JDBCEngineFactory;
//import com.datasqrl.engine.server.GenericJavaServerEngineFactory;
//import com.datasqrl.engine.server.VertxEngineFactory;
//import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.io.formats.JsonLineFormat;
//import com.datasqrl.engine.database.relational.JdbcDataSystemConnector;
//import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
//import com.datasqrl.kafka.KafkaLogEngineFactory;
//import com.datasqrl.packager.Packager;
//import com.datasqrl.io.flexible.schema.FlexibleTableSchemaFactory;
//import java.nio.file.Path;
//import lombok.SneakyThrows;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.configuration.TaskManagerOptions;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
//import picocli.CommandLine;
//
//@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
//public class RunCommand extends AbstractCompilerCommand {
//  EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
//
//  @SneakyThrows
//  @Override
//  public void execute(ErrorCollector errors) {
//    super.execute(errors);
//  }
//
//  @Override
//  public SqrlConfig createDefaultConfig(ErrorCollector errors) {
//    return createRunConfig(root.rootDir, errors);
//  }
//
//  @Override
//  public SqrlConfig postProcessConfig(SqrlConfig config) {
//    //If missing engine config, create one
//    setDefaultConfigs(config);
//
//    // Overwrite the bootstrap servers if one is not specified
//    Value<String> bootstrapServers = config
//        .getSubConfig(PipelineFactory.ENGINES_PROPERTY)
//        .getSubConfig(EngineKeys.LOG)
//        .getSubConfig(CONNECTOR_KEY)
//        .asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
//
//    if (bootstrapServers.getOptional().isEmpty()) {
//      config
//          .getSubConfig(PipelineFactory.ENGINES_PROPERTY)
//          .getSubConfig(EngineKeys.LOG)
//          .getSubConfig(CONNECTOR_KEY)
//          .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:56789");
//    }
//
//    return super.postProcessConfig(config);
//  }
//
//  @Override
//  protected void postprocess(Packager packager, Path targetDir,
//      PhysicalPlan plan, ErrorCollector errors) {
//    super.postprocess(packager, targetDir, plan, errors);
//
//    executePlan(plan, errors);
//  }
//
//  @SneakyThrows
//  protected void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
//    //start services
//    CLUSTER.start();
//    int port = Integer.parseInt(CLUSTER.bootstrapServers().split(":")[1]);
//    PortForwarder forwarder = new PortForwarder(9092, port);
//    new Thread(()->forwarder.start()).start();
//    Runtime.getRuntime().addShutdownHook(new Thread(() -> CLUSTER.stop()));
//
//    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
//    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
//    result.get().get();
//
//    // Hold java open if service is not long running
//    System.in.read();
//  }
//
//  @SneakyThrows
//  protected SqrlConfig createRunConfig(Path rootDir, ErrorCollector errors) {
//    SqrlConfig userConfig = Packager.findPackageFile(rootDir, this.root.packageFiles)
//        .map(p -> SqrlConfigCommons.fromFiles(errors, p))
//        .orElseGet(() -> createLocalConfig(errors));
//
//    setBootstrap(userConfig);
//    return userConfig;
//  }
//
//  public static SqrlConfig createLocalConfig(ErrorCollector errors) {
//    SqrlConfig rootConfig = SqrlConfigCommons.create(errors);
//    return setDefaultConfigs(rootConfig);
//  }
//
//  private static SqrlConfig setDefaultConfigs(SqrlConfig rootConfig) {
//    setDatabaseConfig(rootConfig);
//    setLogConfig(rootConfig);
//    setFlinkConfig(rootConfig);
//    setServerConfig(rootConfig);
//    return rootConfig;
//  }
//
//  private static void setLogConfig(SqrlConfig rootConfig) {
//    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);
//
//    if (!config.hasSubConfig(EngineKeys.LOG)) {
//      SqrlConfig logConfig = config.getSubConfig(EngineKeys.LOG);
//      logConfig.setProperty(EngineFactory.ENGINE_NAME_KEY, KafkaLogEngineFactory.ENGINE_NAME);
//      logConfig.copy(
//          KafkaDataSystemFactory.getKafkaEngineConfig(KafkaLogEngineFactory.ENGINE_NAME,
//              "127.0.0.1:9092",
//              JsonLineFormat.NAME, FlexibleTableSchemaFactory.SCHEMA_TYPE));
//    }
//  }
//
//  private static void setServerConfig(SqrlConfig rootConfig) {
//    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);
//
//    if (!config.hasSubConfig(EngineKeys.SERVER)) {
//      SqrlConfig server = config.getSubConfig(EngineKeys.SERVER);
//      server.setProperty(GenericJavaServerEngineFactory.ENGINE_NAME_KEY,
//          VertxEngineFactory.ENGINE_NAME);
//    }
//  }
//
//  private static void setFlinkConfig(SqrlConfig rootConfig) {
//    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);
//
//    if (!config.hasSubConfig(EngineKeys.STREAMS)) {
//      SqrlConfig flinkConfig = config.getSubConfig(EngineKeys.STREAMS);
//      flinkConfig.setProperty(FlinkEngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);
//      flinkConfig.setProperty(ConfigConstants.LOCAL_START_WEBSERVER, "true");
//      flinkConfig.setProperty(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), "256mb");
//      flinkConfig.setProperty(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "256mb");
//      flinkConfig.setProperty(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "256mb");
//    }
//  }
//
//  private static void setDatabaseConfig(SqrlConfig rootConfig) {
//    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);
//
//    if (!config.hasSubConfig(EngineKeys.DATABASE)) {
//      SqrlConfig dbConfig = config.getSubConfig(EngineKeys.DATABASE);
//      dbConfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
//      dbConfig.setProperties(JdbcDataSystemConnector.builder()
//          .url("jdbc:postgresql://localhost/datasqrl")
//          .driver("org.postgresql.Driver")
//          .dialect("postgres")
//          .database("datasqrl")
//          .host("localhost")
//          .port(5432)
//          .user("postgres")
//          .password("postgres")
//          .build()
//      );
//    }
//  }
//
//  private void setBootstrap(SqrlConfig userConfig) {
//    // Overwrite the bootstrap servers since they change per invocation
//    Value<String> bootstrapServers = userConfig
//        .getSubConfig(PipelineFactory.ENGINES_PROPERTY)
//        .getSubConfig(EngineKeys.LOG)
//        .getSubConfig(CONNECTOR_KEY)
//        .asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
//
//    if (bootstrapServers.getOptional().isEmpty()) {
//      userConfig
//          .getSubConfig(PipelineFactory.ENGINES_PROPERTY)
//          .getSubConfig(EngineKeys.LOG)
//          .getSubConfig(CONNECTOR_KEY)
//          .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//    }
//  }
//}
