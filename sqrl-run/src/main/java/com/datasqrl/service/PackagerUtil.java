package com.datasqrl.service;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.PackagerConfig;
import com.datasqrl.packager.Packager.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    Path mainScript = rootDir.resolve(files[0]);
    errors.checkFatal(Files.isRegularFile(mainScript),
        "Could not find main script: %s", mainScript);
    pkgBuilder.mainScript(mainScript);
    if (files.length > 1) {
      pkgBuilder.graphQLSchemaFile(rootDir.resolve(files[1]));
    }
    return pkgBuilder.build();
  }

  public static final Path DEFAULT_PACKAGE = Path.of("package.json");

  public static List<Path> getOrCreateDefaultPackageFiles(RootCommand root, ErrorCollector errors) {
    Optional<List<Path>> existingPackageJson = findPackageFiles(root.getRootDir(), root.getPackageFiles(), errors);
    return existingPackageJson
            .orElseGet(() -> List.of(writeEngineConfig(root.getRootDir(),
                    createDefaultConfig())));
  }

  @SneakyThrows
  protected static Path writeEngineConfig(Path rootDir, GlobalEngineConfiguration config) {
    Path enginesFile = Files.createTempFile(rootDir, "package-engines", ".json");
    File file = enginesFile.toFile();
    file.deleteOnExit();

    ObjectMapper mapper = new ObjectMapper();
    String enginesConf = mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(config);

    Files.write(enginesFile, enginesConf.getBytes(StandardCharsets.UTF_8));
    return enginesFile;
  }

  protected static GlobalEngineConfiguration createDefaultConfig() {
    JDBCEngineConfiguration jdbcEngineConfiguration = JDBCEngineConfiguration.builder()
            .config(JdbcDataSystemConnectorConfig.builder()
                    .dbURL("jdbc:h2:file:./h2.db")
                    .driverName("org.h2.Driver")
                    .dialect("h2")
                    .database("datasqrl")
                    .build())
            .build();

    FlinkEngineConfiguration flinkEngineConfiguration =
            FlinkEngineConfiguration.builder()
                    .savepoint(false)
                    .build();

    GlobalEngineConfiguration engineConfiguration = GlobalEngineConfiguration.builder()
            .engines(List.of(flinkEngineConfiguration, jdbcEngineConfiguration))
            .build();
    return engineConfiguration;
  }

  public static Optional<List<Path>> findPackageFiles(Path rootDir, List<Path> packageFiles, ErrorCollector errors) {
    if (packageFiles.isEmpty()) {
      Path defaultPkg = rootDir.resolve(DEFAULT_PACKAGE);
      if (Files.isRegularFile(defaultPkg)) {
        return Optional.of(List.of(defaultPkg));
      } else {
        return Optional.empty();
      }
    } else {
      List<Path> resolvedPkgs = new ArrayList<>();
      for (Path pkgPath : packageFiles) {
        Path pkg = rootDir.resolve(pkgPath);
        errors.checkFatal(Files.isRegularFile(pkg),
                "Could not find package file: %s", pkg);
        resolvedPkgs.add(pkg);
      }
      return Optional.of(resolvedPkgs);
    }
  }


}
