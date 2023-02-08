package com.datasqrl.service;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.PackagerConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;

public class PackagerUtil {

  @SneakyThrows
  public static Packager create(Path rootDir, Path[] files, List<Path> packageFiles, ErrorCollector errors) {
    errors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");
    PackagerConfig packagerConfig = createPackageConfig(files, rootDir, packageFiles, errors);
    return packagerConfig.getPackager(errors);
  }

  protected static PackagerConfig createPackageConfig(Path[] files, Path rootDir, List<Path> packageFiles,
      ErrorCollector errors) {
    PackagerConfig.PackagerConfigBuilder pkgBuilder = PackagerConfig.builder();
    pkgBuilder.rootDir(rootDir);
    pkgBuilder.packageFiles(packageFiles);
    Path mainScript = files[0];
    errors.checkFatal(mainScript != null && Files.isRegularFile(mainScript),
        "Could not find main script: %s", mainScript);
    pkgBuilder.mainScript(mainScript);
    if (files.length > 1) {
      pkgBuilder.graphQLSchemaFile(files[1]);
    }

    return pkgBuilder.build();
  }
}
