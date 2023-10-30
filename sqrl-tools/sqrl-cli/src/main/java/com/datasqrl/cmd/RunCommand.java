/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true, true, true);
  }


  public void runCommand(ErrorCollector errors) {
    if (DEFAULT_DEPLOY_DIR.equals(targetDir)) {
      targetDir = root.rootDir.resolve(targetDir);
    }

    DefaultConfigSupplier configSupplier = new DefaultConfigSupplier(errors);

    SqrlConfig config = getDefaultConfig(true, errors)
        .get();

    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);
    packager.cleanUp();
    Path packageFilePath =  packager.populateBuildDir(!noinfer);
    if (errors.hasErrors()) {
      return;
    }

    Compiler compiler = new Compiler();
    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));
    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug, targetDir);

    if (errors.hasErrors()) {
      return;
    }
    if (configSupplier.usesDefault) {
      addDockerCompose(Optional.ofNullable(mountDirectory));
      addFlinkExecute();
    }
    if (isGenerateGraphql()) {
      addGraphql(packager.getBuildDir(), packager.getRootDir());
    }

    executePlan(result.getPlan(), errors);
  }
}
