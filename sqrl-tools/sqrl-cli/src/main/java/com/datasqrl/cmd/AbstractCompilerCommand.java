/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.FlinkCodeGen;
import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.compile.DockerCompose;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
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
  private boolean kafkaStarted;

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql, boolean startKafka) {
    this.execute = execute;
    this.startGraphql = startGraphql;
    this.startKafka = startKafka;
  }

  @SneakyThrows
  public void runCommand(ErrorCollector errors) {
    setupTargetDir();

    DefaultConfigSupplier configSupplier = new DefaultConfigSupplier(errors);
    SqrlConfig config = initializeConfig(configSupplier, errors);
    Pair<Packager, Path> packager = createPackager(config, errors);
    if (errors.hasErrors()) {
      return;
    }
    Compiler.CompilerResult result = compilePackage(packager.getRight(), errors);
    if (errors.hasErrors()) {
      return;
    }

    postCompileActions(configSupplier, packager.getLeft(), result, errors);
  }

  protected void setupTargetDir() {
    if (DEFAULT_DEPLOY_DIR.equals(targetDir)) {
      targetDir = root.rootDir.resolve(targetDir);
    }
  }

  protected SqrlConfig initializeConfig(DefaultConfigSupplier configSupplier, ErrorCollector errors) {
    return PackagerUtil.getOrCreateDefaultConfiguration(root, errors, configSupplier);
  }

  protected Pair<Packager, Path> createPackager(SqrlConfig config, ErrorCollector errors) {
    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);
    packager.cleanUp();
    Path path = packager.populateBuildDir(!noinfer);
    return Pair.of(packager, path);
  }

  protected Compiler.CompilerResult compilePackage(Path packageFilePath, ErrorCollector errors) {
    Compiler compiler = new Compiler();
    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));
    return compiler.run(errors, packageFilePath.getParent(), debug, targetDir);
  }

  protected void postCompileActions(DefaultConfigSupplier configSupplier, Packager packager,
      CompilerResult result, ErrorCollector errors) {
    if (configSupplier.usesDefault) {
      addDockerCompose(Optional.ofNullable(mountDirectory));
      addFlinkExecute();
    }
    if (isGenerateGraphql()) {
      addGraphql(packager.getBuildDir(), packager.getRootDir());
    }
    addFlinkCodeGenSample(packager.getBuildDir(), result);
  }

  @SneakyThrows
  private void addFlinkCodeGenSample(Path buildDir, CompilerResult result) {
    try {
      FlinkCodeGen codeGen = new FlinkCodeGen();
      FlinkExecutablePlan executablePlan = result.getPlan()
          .getPlans(FlinkStreamPhysicalPlan.class).findFirst().get()
          .getExecutablePlan();
      TypeSpec accept = executablePlan.accept(codeGen, null);
      Files.writeString(buildDir.resolve("FlinkMainSample.java"),
          accept.toString());
    } catch (Exception e) {
      //Allow failures
    }
  }

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(Path build, Path rootDir) {
    Files.copy(build.resolve(GRAPHQL_NORMALIZED_FILE_NAME),
        rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME));
  }

  //Adds in regardless
  protected void addDockerCompose(Optional<Path> mountDir) {
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

  protected void addFlinkExecute() {
    String sh = DockerCompose.getFlinkExecute();
    Path toFile = targetDir.resolve("submit-flink-job.sh");
    try {
      Files.createDirectories(targetDir);
      Files.writeString(toFile, sh);

      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
      Files.setPosixFilePermissions(toFile, perms);

    } catch (Exception e) {
      log.error("Could not copy flink executor file.");
      throw new RuntimeException(e);
    }
  }

  protected Supplier<SqrlConfig> getDefaultConfig(boolean startKafka, ErrorCollector errors) {
    if (startKafka) {
      startKafka();
      return ()->PackagerUtil.createLocalConfig(CLUSTER.bootstrapServers(), errors);
    }

    //Generate docker config
    return ()->PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
  }

  protected class DefaultConfigSupplier implements Supplier<SqrlConfig> {

    boolean usesDefault = false;
    final ErrorCollector errors;

    protected DefaultConfigSupplier(ErrorCollector errors) {
      this.errors = errors;
    }

    @Override
    public SqrlConfig get() {
      usesDefault = true;
      return PackagerUtil.createDockerConfig(root.rootDir, targetDir, errors);
    }
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
  protected void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    Predicate<ExecutionStage> stageFilter = s -> true;
    if (!startGraphql) stageFilter = s -> s.getEngine().getType()!= Type.SERVER;
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
    result.get().get();

    // Hold java open if service is not long running
    System.in.read();
  }
}
