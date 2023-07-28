/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.DockerCompose;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

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

  @CommandLine.Option(names = {"--mnt"}, description = "Local directory to mount for data access")
  protected Path mountDirectory = null;

  @CommandLine.Option(names = {"--nolookup"}, description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql, boolean startKafka) {
    this.execute = execute;
    this.startGraphql = startGraphql;
    this.startKafka = startKafka;
  }

  public void runCommand(ErrorCollector errors) {
    if (DEFAULT_DEPLOY_DIR.equals(targetDir)) {
      targetDir = root.rootDir.resolve(targetDir);
    }
    DefaultConfigSupplier configSupplier = new DefaultConfigSupplier(errors);
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors, configSupplier);

    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);
    packager.cleanUp();
    Path packageFilePath =  packager.populateBuildDir(!noinfer);

    Compiler compiler = new Compiler();
    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));
    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug, targetDir);

    if (configSupplier.usesDefault) {
      addDockerCompose(Optional.ofNullable(mountDirectory));
    }
    if (isGenerateGraphql()) {
      addGraphql(packager.getBuildDir(), packager.getRootDir());
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
  private void addDockerCompose(Optional<Path> mountDir) {
    String yml = DockerCompose.getYml(mountDir);
    Path toFile = targetDir.resolve("docker-compose.yml");
    try {
      Files.createDirectories(targetDir);
      Files.writeString(toFile, yml);
    } catch (Exception e) {
      log.error("Could not copy docker-compose file.");
      throw new RuntimeException(e);
    }
  }

  private class DefaultConfigSupplier implements Supplier<SqrlConfig> {

    boolean usesDefault = false;
    final ErrorCollector errors;

    private DefaultConfigSupplier(ErrorCollector errors) {
      this.errors = errors;
    }

    @Override
    public SqrlConfig get() {
      usesDefault = true;
      return PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
    }
  }

  private void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    Predicate<ExecutionStage> stageFilter = s -> true;
    if (!startGraphql) stageFilter = s -> s.getEngine().getType()!= Type.SERVER;
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
    result.get();
  }
}
