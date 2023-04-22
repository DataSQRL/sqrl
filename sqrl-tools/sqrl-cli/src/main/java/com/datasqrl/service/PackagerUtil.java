package com.datasqrl.service;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.PackagerConfig;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class PackagerUtil {

  @SneakyThrows
  public static Packager create(Path rootDir, Path[] files, SqrlConfig config,
      ErrorCollector errors) {
    errors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");
    PackagerConfig packagerConfig = createPackageConfig(files, rootDir, config);
    return packagerConfig.getPackager(errors);
  }

  protected static PackagerConfig createPackageConfig(Path[] files, Path rootDir, SqrlConfig config) {
    PackagerConfig.PackagerConfigBuilder pkgBuilder = PackagerConfig.builder();
    pkgBuilder.rootDir(rootDir);
    pkgBuilder.config(config);
    pkgBuilder.mainScript(files[0]);
    if (files.length > 1) {
      pkgBuilder.graphQLSchemaFile(files[1]);
    }
    return pkgBuilder.build();
  }

  public static final Path DEFAULT_PACKAGE = Path.of(Packager.PACKAGE_FILE_NAME);

  public static SqrlConfig getOrCreateDefaultConfiguration(RootCommand root, ErrorCollector errors) {
    List<Path> configFiles = getOrCreateDefaultPackageFiles(root,errors);
    Preconditions.checkArgument(configFiles.size()>=1);
    return SqrlConfigCommons.fromFiles(errors,configFiles.get(0),configFiles.subList(1,configFiles.size()).stream().toArray(Path[]::new));
  }

  public static List<Path> getOrCreateDefaultPackageFiles(RootCommand root, ErrorCollector errors) {
    Optional<List<Path>> existingPackageJson = findRootPackageFiles(root);
    return existingPackageJson
            .orElseGet(() -> List.of(writeEngineConfig(root.getRootDir(),
                    createDefaultConfig(errors))));
  }

  @SneakyThrows
  protected static Path writeEngineConfig(Path rootDir, SqrlConfig config) {
    Path enginesFile = Files.createTempFile(rootDir, "package-engines", ".json");
    File file = enginesFile.toFile();
    file.deleteOnExit();

    config.toFile(enginesFile,true);
    return enginesFile;
  }

  protected static SqrlConfig createDefaultConfig(ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.create(errors)
        .getSubConfig(PipelineFactory.ENGINES_PROPERTY);

    SqrlConfig dbConfig = config.getSubConfig("database");
    dbConfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
    dbConfig.setProperties(JdbcDataSystemConnector.builder()
        .dbURL("jdbc:h2:file:./h2.db")
        .driverName("org.h2.Driver")
        .dialect("h2")
        .database("datasqrl")
        .build()
    );

    SqrlConfig flinkConfig = config.getSubConfig("stream");
    flinkConfig.setProperty(FlinkEngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);

    return config;
  }

  public static Optional<List<Path>> findRootPackageFiles(RootCommand root) {
    return findPackageFiles(root.getRootDir(), root.getPackageFiles());
  }

  public static Optional<List<Path>> findPackageFiles(Path rootDir, List<Path> packageFiles) {
    if (packageFiles.isEmpty()) {
      Path defaultPkg = rootDir.resolve(DEFAULT_PACKAGE);
      if (Files.isRegularFile(defaultPkg)) {
        return Optional.of(List.of(defaultPkg));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.of(packageFiles);
    }
  }


}
