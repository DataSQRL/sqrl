package com.datasqrl.cmd;

import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_DEPLOY_DIR;

import com.datasqrl.compile.Compiler;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@Slf4j
@CommandLine.Command(name = "generate-assets", description = "Generates build assets")
public class GenerateAssetsCommand extends AbstractCommand {

  //TODO: Unify this config
  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) API specification")
  protected Path[] files;

  @CommandLine.Option(names = {"-e",
      "--engine"}, description = "Generates a build asset for an engine")
  private String targetEngine;

  @CommandLine.Option(names = {"-t",
      "--target"}, description = "Target directory for deployment artifacts")
  protected Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {
      "--nolookup"}, description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;

  @CommandLine.Option(names = {"-d",
      "--debug"}, description = "Outputs table changestream to configured sink for debugging")
  protected boolean debug = false;

  @Override
  public void runCommand(ErrorCollector errors) {
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors,
        null);
    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);

    Build build = new Build(errors);
    Path packageFilePath = build.build(packager, !noinfer);

    Compiler compiler = new Compiler();

    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));

    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug,
        targetDir);

    Set<ExecutionEngine> engines = result.getPlan().getStagePlans().stream()
        .map(e -> e.getStage().getEngine())
        .collect(Collectors.toSet());

    for (ExecutionEngine engine : engines) {
      if (shouldGenerateAssets(targetEngine, engine)) {
        log.info("Generating assets for engine: " + targetEngine);
        engine.generateAssets(packageFilePath.getParent());
      }
    }
  }

  private boolean shouldGenerateAssets(String targetEngine, ExecutionEngine engine) {
    return targetEngine == null || engine.getName().equalsIgnoreCase(targetEngine);
  }
}
