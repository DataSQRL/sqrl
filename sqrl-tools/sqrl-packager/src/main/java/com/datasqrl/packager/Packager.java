/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineKeys;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.server.GenericJavaServerEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.engine.kafka.KafkaLogEngineFactory;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.DependencyConfig;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.packager.preprocess.DataSystemPreprocessor;
import com.datasqrl.packager.preprocess.FlinkSqlPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.PackageJsonPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.TablePreprocessor;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
public class Packager {
  public static final String BUILD_DIR_NAME = "build";
  public static final String DEPLOY_DIR_NAME = "deploy";
  public static final String PROFILES_KEY = "profiles";
  public static final String PACKAGE_JSON = "package.json";
  public static final Path DEFAULT_PACKAGE = Path.of(Packager.PACKAGE_JSON);

  private final Repository repository;
  private final Path rootDir;
  private final SqrlConfig config;
  private final Path buildDir;
  private final ErrorCollector errors;

  public Packager(@NonNull Repository repository, @NonNull Path rootDir,
      @NonNull SqrlConfig sqrlConfig, @NonNull ErrorCollector errors) {
    errors.checkFatal(Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    Preconditions.checkArgument(Files.isDirectory(rootDir));
    this.repository = repository;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(BUILD_DIR_NAME);
    this.errors = errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON));
    this.config = sqrlConfig;
  }

  public Path preprocess() {
    errors.checkFatal(
        ScriptConfiguration.fromScriptConfig(config).asString(ScriptConfiguration.MAIN_KEY)
            .getOptional().map(StringUtils::isNotBlank).orElse(false),
        "No config or main script specified");
    try {
      cleanBuildDir(buildDir);
      createBuildDir(buildDir);
      retrieveDependencies();
      copyFilesToBuildDir();
      preProcessFiles(config);
      writePackageConfig();
      return buildDir.resolve(PACKAGE_JSON);
    } catch (IOException e) {
      throw errors.handle(e);
    }
  }

  @SneakyThrows
  public static void createBuildDir(Path buildDir) {
    Files.createDirectories(buildDir);
  }

  /**
   * Helper function for retrieving listed dependencies.
   */
  private void retrieveDependencies() {
    LinkedHashMap<String, Dependency> dependencies = DependencyConfig.fromRootConfig(config);
    ErrorCollector depErrors = config.getErrorCollector()
        .resolve(DependencyConfig.DEPENDENCIES_KEY);
    dependencies.entrySet().stream()
        .map(entry -> rethrowCall(() ->
            retrieveDependency(buildDir, NamePath.parse(entry.getKey()),
                entry.getValue().normalize(entry.getKey(), depErrors))
                ? Optional.<NamePath>empty()
                : Optional.of(NamePath.parse(entry.getKey()))))
        .flatMap(Optional::stream)
        .forEach(failedDep -> depErrors.fatal("Could not retrieve dependency: %s", failedDep));
  }

  private void copyFilesToBuildDir() throws IOException {
    Map<String, Optional<Path>> destinationPaths = copyScriptFilesToBuildDir().entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey, v->canonicalizePath(v.getValue())));
    //Files should exist, if error occurs its internal, hence we create root error collector
    addFileToPackageJsonConfig(buildDir, ScriptConfiguration.fromScriptConfig(config),
        destinationPaths, errors);

    // Shouldn't be here, move to postprocessor for flink
    String buildFile = FileUtil.readResource("build.gradle");
    Files.copy(new ByteArrayInputStream(buildFile.getBytes()),
        buildDir.resolve("build.gradle"));
  }

  public static void addFileToPackageJsonConfig(Path rootDir, SqrlConfig config, Map<String, Optional<Path>> filesByKey,
      ErrorCollector errors) {
    filesByKey.forEach((key, file) -> {
      if (file.isPresent()) {
        errors.checkFatal(Files.isRegularFile(file.get()), "Could not locate %s file: %s", key, file.get());
        config.setProperty(key,rootDir.relativize(file.get()).normalize().toString());
      }
    });
  }

  public static Optional<Path> canonicalizePath(Optional<Path> path) {
     return path.map(Packager::canonicalizePath);
  }

  public static Path canonicalizePath(Path path) {
     return Path.of(path.toString().toLowerCase());
  }

  /**
   * Copies all the files in the script configuration section of the config to the build dir
   * and either normalizes the file or preserves the relative path.
   *
   * @throws IOException
   */
  private Map<String, Optional<Path>> copyScriptFilesToBuildDir() throws IOException {
    SqrlConfig scriptConfig = ScriptConfiguration.fromScriptConfig(config);
    Map<String, Optional<Path>> destinationPaths = new HashMap<>();
    for (String fileKey : ScriptConfiguration.FILE_KEYS) {
      Optional<String> configuredFile = scriptConfig.asString(fileKey).getOptional();
      if (configuredFile.isPresent()) {
        Path destinationPath = copyRelativeFile(rootDir.resolve(configuredFile.get()), rootDir,
            buildDir);
        destinationPaths.put(fileKey,Optional.of(destinationPath));
      }
    }
    return destinationPaths;
  }

  /**
   * Helper function to preprocess files.
   */
  private void preProcessFiles(SqrlConfig config) throws IOException {
    //Preprocessor will normalize files
    List<Preprocessor> processorList = ListUtils.union(List.of(new TablePreprocessor(),
            new JarPreprocessor(), new DataSystemPreprocessor(), new PackageJsonPreprocessor(),
            new FlinkSqlPreprocessor()),
        ServiceLoaderDiscovery.getAll(Preprocessor.class));
    Preprocessors preprocessors = new Preprocessors(processorList, errors);
    preprocessors.handle(
        PreprocessorsContext.builder()
            .rootDir(rootDir)
            .buildDir(buildDir)
            .config(config)
            .build());
  }

  private void writePackageConfig() throws IOException {
    config.toFile(buildDir.resolve(PACKAGE_JSON), true);
  }

  public static void cleanBuildDir(Path buildDir) throws IOException {
    if (Files.exists(buildDir) && Files.isDirectory(buildDir)) {
      Files.walk(buildDir)
          // Sort the paths in reverse order so that directories are deleted last
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } else if (Files.exists(buildDir) && !Files.isDirectory(buildDir)) {
      buildDir.toFile().delete();
    }
  }

  private boolean retrieveDependency(Path buildDir, NamePath packagePath, Dependency dependency)
      throws IOException {
    Path targetPath = namepath2Path(buildDir, packagePath);
    Preconditions.checkArgument(FileUtil.isEmptyDirectory(targetPath),
        "Dependency [%s] conflicts with existing module structure in directory: [%s]", dependency,
        targetPath);
    return repository.retrieveDependency(targetPath, dependency);
  }

  public static Path copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
    return copyFile(srcFile, destDir, srcDir.relativize(srcFile));
  }

  public static Path copyFile(Path srcFile, Path destDir, Path relativeDestPath)
      throws IOException {
    Preconditions.checkArgument(Files.isRegularFile(srcFile), "Is not a file: %s", srcFile);
    Path targetPath = canonicalizePath(destDir.resolve(relativeDestPath));
    if (!Files.exists(targetPath.getParent())) {
      Files.createDirectories(targetPath.getParent());
    }
    Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }

  public void postprocess(Path rootDir, Path targetDir, Optional<Path> mountDirectory,
      String[] profiles) {

    // Copy profiles
    for (String profile : profiles) {
      copyToDeploy(targetDir,
          rootDir.resolve(profile));
    }
//    List.of()
//        .forEach(p->p.process(new ProcessorContext(buildDir, targetDir, mountDirectory, profiles)));
  }

  @SneakyThrows
  private void copyToDeploy(Path targetDir, Path profile) {
    if (!Files.exists(targetDir)) {
      Files.createDirectories(targetDir);
    }

    // Copy each file and directory from the profile path to the target directory
    try (Stream<Path> stream = Files.walk(profile)) {
      stream.forEach(sourcePath -> {
        Path destinationPath = targetDir.resolve(profile.relativize(sourcePath))
            .toAbsolutePath();
        if (Files.isDirectory(sourcePath)) {
          try {
            Files.createDirectories(destinationPath);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {

          try {
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      });
    }
  }

  @SneakyThrows
  public static Path writeEngineConfig(Path rootDir, SqrlConfig config) {
    Path enginesFile = Files.createTempFile(rootDir, "package-engines", ".json");
    File file = enginesFile.toFile();
    file.deleteOnExit();

    config.toFile(enginesFile, true);
    return enginesFile;
  }

  public static Optional<List<Path>> findPackageFile(Path rootDir, List<Path> packageFiles) {
    if (packageFiles.isEmpty()) {
      Path defaultPkg = rootDir.resolve(DEFAULT_PACKAGE);
      if (Files.isRegularFile(defaultPkg)) {
        return Optional.of(List.of(defaultPkg));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.of(packageFiles.stream().map(rootDir::resolve).collect(Collectors.toUnmodifiableList()));
    }
  }

  public static SqrlConfig createDockerConfig(ErrorCollector errors) {
    SqrlConfig rootConfig = SqrlConfig.createCurrentVersion(errors);

    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);

    SqrlConfig dbConfig = config.getSubConfig("database");
    dbConfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
    dbConfig.setProperties(JdbcDataSystemConnector.builder()
        .url("jdbc:postgresql://database:5432/datasqrl")
        .driver("org.postgresql.Driver")
        .dialect("postgres")
        .database("datasqrl")
        .user("postgres")
        .password("postgres")
        .host("database")
        .port(5432)
        .build()
    );
    SqrlConfig flinkConfig = config.getSubConfig(EngineKeys.STREAMS);
    flinkConfig.setProperty(FlinkEngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);

    SqrlConfig server = config.getSubConfig(EngineKeys.SERVER);
    server.setProperty(GenericJavaServerEngineFactory.ENGINE_NAME_KEY,
        VertxEngineFactory.ENGINE_NAME);

    SqrlConfig logConfig = config.getSubConfig(EngineKeys.LOG);
    logConfig.setProperty(EngineFactory.ENGINE_NAME_KEY, KafkaLogEngineFactory.ENGINE_NAME);
    logConfig.copy(
        KafkaDataSystemFactory.getKafkaEngineConfig(KafkaLogEngineFactory.ENGINE_NAME, "kafka:9092",
            JsonLineFormat.NAME, FlexibleTableSchemaFactory.SCHEMA_TYPE));

    return rootConfig;
  }

  public static void setScriptFiles(Path rootDir, Path mainScript, Path graphQLSchemaFile,
      SqrlConfig sqrlConfig, ErrorCollector errors) {
    errors.checkFatal(mainScript == null || Files.isRegularFile(mainScript), "Could not find sqrl file: %s", mainScript);
    errors.checkFatal(!(graphQLSchemaFile != null && !Files.isRegularFile(graphQLSchemaFile)), "Could not find API file: %s", graphQLSchemaFile);

    SqrlConfig scriptConfig = ScriptConfiguration.fromScriptConfig(sqrlConfig);
    addFileToPackageJsonConfig(rootDir, scriptConfig, Map.of(ScriptConfiguration.MAIN_KEY, Optional.ofNullable(mainScript),
        ScriptConfiguration.GRAPHQL_KEY, Optional.ofNullable(graphQLSchemaFile)), errors);
  }
}
