package com.datasqrl.packager.config;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.service.PackagerUtil;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.SneakyThrows;

public class DefaultConfig {
//
//  //happens at init
//  private void updateScriptConfig(ErrorCollector errors) {
//    SqrlConfig scriptConfig = ScriptConfiguration.fromRootConfig(config);
//    setScriptFiles(rootDir, scriptConfig, ImmutableMap.of(ScriptConfiguration.MAIN_KEY, Optional.ofNullable(mainScript),
//        ScriptConfiguration.GRAPHQL_KEY, Optional.ofNullable(graphQLSchemaFile)), errors);
//  }
//
//  protected SqrlConfig initializeConfig(DefaultConfigSupplier configSupplier, ErrorCollector errors) {
//    return PackagerUtil.getOrCreateDefaultConfiguration(root, errors, configSupplier);
//  }
//
//  protected Supplier<SqrlConfig> getDefaultConfig(boolean startKafka, ErrorCollector errors) {
//    if (startKafka) {
//      startKafka();
//      return ()->PackagerUtil.createLocalConfig(CLUSTER.bootstrapServers(), errors);
//    }
//
//    //Generate docker config
//    return ()->PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
//  }
//
//  protected class DefaultConfigSupplier implements Supplier<SqrlConfig> {
//
//    boolean usesDefault = false;
//    final ErrorCollector errors;
//
//    protected DefaultConfigSupplier(ErrorCollector errors) {
//      this.errors = errors;
//    }
//
//    @Override
//    public SqrlConfig get() {
//      usesDefault = true;
//      return PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
//    }
//  }
//
//  private void startKafka() {
//    //We're generating an embedded config, start the cluster
//    try {
//      if (!kafkaStarted) {
//        CLUSTER.start();
//      }
//      this.kafkaStarted = true;
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @SneakyThrows
//  protected void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
//    Predicate<ExecutionStage> stageFilter = s -> true;
//    if (!startGraphql) stageFilter = s -> s.getEngine().getType()!= Type.SERVER;
//    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
//    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
//    result.get().get();
//
//    // Hold java open if service is not long running
//    System.in.read();
//  }
}
