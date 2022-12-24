package com.datasqrl.service;

import com.datasqrl.packager.Packager;
import com.datasqrl.packager.Packager.Config;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class PackagerUtil {

  @SneakyThrows
  public static Packager create(Path rootDir, Path[] files, List<Path> packageFiles) {
    Packager.Config packagerConfig = createPackageConfig(files, rootDir, packageFiles);

    return packagerConfig.getPackager();
  }

  protected static Config createPackageConfig(Path[] files, Path rootDir, List<Path> packageFiles) {
    Packager.Config.ConfigBuilder pkgBuilder = Packager.Config.builder();
    pkgBuilder.rootDir(rootDir);
    pkgBuilder.packageFiles(packageFiles);
    Path mainScript = files[0];
    Preconditions.checkArgument(mainScript != null && Files.isRegularFile(mainScript),
        "Could not find main script: %s", mainScript);
    pkgBuilder.mainScript(mainScript);
    if (files.length > 1) {
      pkgBuilder.graphQLSchemaFile(files[1]);
    }

    return pkgBuilder.build();
  }
}
