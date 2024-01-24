/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.EngineKeys;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.server.GenericJavaServerEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.packager.ImportExportAnalyzer.Result;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.DependencyConfig;
import com.datasqrl.packager.postprocess.DockerPostprocessor;
import com.datasqrl.packager.postprocess.FlinkPostprocessor;
import com.datasqrl.packager.postprocess.Postprocessor.ProcessorContext;
import com.datasqrl.packager.preprocess.DataSystemPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.TablePreprocessor;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;

@Getter
public class Packager {
  public static final String BUILD_DIR_NAME = "build";

  // this is the default package name, but not the specified package name.
  public static final String PACKAGE_JSON = "package.json";

  public static final Path DEFAULT_PACKAGE = Path.of(Packager.PACKAGE_JSON);

  Repository repository;
  Path rootDir;
  // remove config?
  SqrlConfig config;
  ErrorCollector errors;
  Path buildDir;
  String[] profiles;
  private final Path mainScript;
  private final List<Path> packageFiles;
  private final Path graphQLSchemaFile;
  private final Optional<Path> mountDirectory;

  public Packager(@NonNull Repository repository,
      @NonNull Path rootDir,
      Path mainScript,
      List<Path> packageFiles,
      Path graphQLSchemaFile,
      String[] profiles,
      Optional<Path> mountDirectory, @NonNull ErrorCollector errors) {
    this.mainScript = mainScript;
    this.packageFiles = packageFiles;
    this.graphQLSchemaFile = graphQLSchemaFile;
    this.mountDirectory = mountDirectory;
    errors.checkFatal(Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    errors.checkFatal(graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile), "Could not find API file: %s", graphQLSchemaFile);

    this.profiles = profiles;
    Preconditions.checkArgument(Files.isDirectory(rootDir));
    this.repository = repository;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(BUILD_DIR_NAME);
    cleanUp();
    createBuildDir();

    this.errors = errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON));
    this.config = handleProfileConfigs();
  }

  public Path preprocess(boolean inferDependencies) {
    errors.checkFatal(
        ScriptConfiguration.fromScriptConfig(config).asString(ScriptConfiguration.MAIN_KEY)
            .getOptional().map(StringUtils::isNotBlank).orElse(false),
        "No config or main script specified");
    try {
      cleanUp();
      cleanBuildDir();
      createBuildDir();
      if (inferDependencies) {
        inferDependencies();
      }
      retrieveDependencies();
      copyFilesToBuildDir();
      preProcessFiles(config);
      writePackageConfig();
      return buildDir.resolve(PACKAGE_JSON);
    } catch (IOException e) {
      throw errors.handle(e);
    }
  }

  private SqrlConfig handleProfileConfigs() {
    List<Path> configFiles = new ArrayList<>();

    //explicit cli package overrides
    if (!packageFiles.isEmpty()) {
      configFiles.addAll(packageFiles);
    } else { //implicit package json
      if (Files.exists(rootDir.resolve(PACKAGE_JSON))) {
        configFiles.add(rootDir.resolve(PACKAGE_JSON));
      }
    }

    // Add profile config files
    for (String profile : profiles) {
      Path profilePath = rootDir.resolve(profile).resolve(PACKAGE_JSON);
      if (Files.isRegularFile(profilePath)) {
        configFiles.add(profilePath);
      } else if (isSystemProfile(profile)) {
        if (packageFiles.isEmpty()) { //create default
          if (profile.equalsIgnoreCase("compile")) {
            SqrlConfig dockerConfig = createDockerConfig(errors);
            Path path = buildDir.resolve(PACKAGE_JSON);
            dockerConfig.toFile(path);
            configFiles.add(path);
          } else if (profile.equalsIgnoreCase("run")) {
            SqrlConfig dockerConfig = createDockerConfig(errors);
            dockerConfig.toFile(buildDir.resolve(PACKAGE_JSON));
          }
        }
      } else {
        // Handle error or log missing profile config
        throw errors.handle(new IOException("Profile config not found: " + profile));
      }
    }

    // Could not find any package json
    if (configFiles.isEmpty()) {
      throw new RuntimeException("Could not find package.json");
    }

    // Merge all configurations
    SqrlConfig sqrlConfig = SqrlConfigCommons.fromFiles(errors, configFiles);

    setScriptFiles(rootDir, mainScript, graphQLSchemaFile, sqrlConfig, errors);

    return sqrlConfig;
  }

  public static void setScriptFiles(Path rootDir, Path mainScript, Path graphQLSchemaFile,
      SqrlConfig sqrlConfig, ErrorCollector errors) {
    SqrlConfig scriptConfig = ScriptConfiguration.fromScriptConfig(sqrlConfig);
    addFileToPackageJsonConfig(rootDir, scriptConfig, Map.of(ScriptConfiguration.MAIN_KEY, Optional.ofNullable(mainScript),
        ScriptConfiguration.GRAPHQL_KEY, Optional.ofNullable(graphQLSchemaFile)), errors);
  }

  private boolean isSystemProfile(String profile) {
    return profile.equalsIgnoreCase("compile") || profile.equalsIgnoreCase("run");
  }

  public static SqrlConfig createDockerConfig(ErrorCollector errors) {
    SqrlConfig rootConfig = SqrlConfigCommons.create(errors);

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

  @SneakyThrows
  private void createBuildDir() {
    Files.createDirectories(buildDir);
  }

  //Pass in dependencies ?
  private void inferDependencies() throws IOException {
    //Analyze all local SQRL files to discovery transitive or undeclared dependencies
    //At the end, we'll add the new dependencies to the package config.
    ImportExportAnalyzer analyzer = new ImportExportAnalyzer();

    BiPredicate<Path, BasicFileAttributes> FIND_SQRL_SCRIPT = (p, f) ->
        f.isRegularFile() && p.getFileName().toString().toLowerCase().endsWith(".sqrl");

    // Find all SQRL script files
    Result allResults = Files.find(rootDir, 128, FIND_SQRL_SCRIPT)
        .map(script -> analyzer.analyze(script, errors))
        .reduce(Result.EMPTY, Result::add);

    StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
    Set<NamePath> pkgs = new HashSet<>(allResults.getPkgs());
    pkgs.removeAll(standardLibraryLoader.loadedLibraries());
    pkgs.remove(Name.system(PrintDataSystemFactory.SYSTEM_NAME).toNamePath());

    Set<NamePath> unloadedDeps = new HashSet<>();
    for (NamePath packagePath : pkgs) {
      Path dir = namepath2Path(rootDir, packagePath);
      if (!Files.exists(dir)) {
        unloadedDeps.add(packagePath);
      }
    }

    LinkedHashMap<String, Dependency> inferredDependencies = new LinkedHashMap<>();

    //Resolve dependencies
    for (NamePath unloadedDep : unloadedDeps) {
      repository
          .resolveDependency(unloadedDep.toString())
          .ifPresentOrElse((dep) -> inferredDependencies.put(unloadedDep.toString(), dep),
              () -> errors.checkFatal(true, "Could not infer dependency: %s", unloadedDep));
    }

    // Add inferred dependencies to package config
    inferredDependencies.forEach((key, dep) -> {
      config.getSubConfig(DependencyConfig.DEPENDENCIES_KEY).getSubConfig(key).setProperties(dep);
    });
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
        destinationPaths, ErrorCollector.root());

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
            new JarPreprocessor(), new DataSystemPreprocessor()),
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

  //Todo; remove for cleanUp
  private void cleanBuildDir() throws IOException {
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

  public void cleanUp() {
    try {
      Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
      if (Files.exists(buildDir)) {
        FileUtils.deleteDirectory(buildDir.toFile());
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not read or write files on local file-system", e);
    }
  }

  public static Path copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
    return copyFile(srcFile, destDir, srcDir.relativize(srcFile));
  }

  public static Path copyFile(Path srcFile, Path destDir, Path relativeDestPath)
      throws IOException {
    Preconditions.checkArgument(Files.isRegularFile(srcFile), "Is not a file: {}", srcFile);
    Path targetPath = canonicalizePath(destDir.resolve(relativeDestPath));
    if (!Files.exists(targetPath.getParent())) {
      Files.createDirectories(targetPath.getParent());
    }
    Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }

  public void postprocess(CompilerResult result, Path targetDir) {
    List.of(new DockerPostprocessor(), new FlinkPostprocessor())
        .forEach(p->p.process(new ProcessorContext(targetDir, result, mountDirectory)));
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
      return Optional.of(packageFiles);
    }
  }
}
