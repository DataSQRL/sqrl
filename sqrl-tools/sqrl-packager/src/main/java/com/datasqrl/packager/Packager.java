/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.DependenciesConfigImpl;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.PackageJson.DependenciesConfig;
import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.datasqrl.config.RootPath;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.StagePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import freemarker.template.Configuration;
import freemarker.template.DefaultMapAdapter;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datasqrl.actions.WriteDag.DATA_DIR;
import static com.datasqrl.actions.WriteDag.LIB_DIR;
import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_KEY;
import static com.datasqrl.config.ScriptConfigImpl.MAIN_KEY;
import static com.datasqrl.packager.LambdaUtil.rethrowCall;
import static com.datasqrl.util.NameUtil.namepath2Path;

@Getter
@AllArgsConstructor(onConstructor_=@Inject)
public class Packager {
  public static final String BUILD_DIR_NAME = "build";
  public static final String DEPLOY_DIR_NAME = "deploy";
  public static final String PLAN_DIR_NAME = "plan";
  public static final String PACKAGE_JSON = "package.json";
  public static final Path DEFAULT_PACKAGE = Path.of(Packager.PACKAGE_JSON);

  private final Repository repository;
  private final RootPath rootDir;
  private final PackageJson config;
  private final BuildPath buildDir;
  private final Preprocessors preprocessors;
  public void preprocess(ErrorCollector errors) {
    errors.checkFatal(
        config.getScriptConfig().getMainScript().map(StringUtils::isNotBlank).orElse(false),
        "No config or main script specified");
    try {
      cleanBuildDir(buildDir.getBuildDir());
      createBuildDir(buildDir.getBuildDir());
      retrieveDependencies(errors);
      copyFilesToBuildDir(errors);
      preProcessFiles(config, errors);
      writePackageConfig();
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
  private void retrieveDependencies(ErrorCollector errors) {
    DependenciesConfig dependencies = config.getDependencies();
    ErrorCollector depErrors = errors
        .resolve(DependenciesConfigImpl.DEPENDENCIES_KEY);

    dependencies.getDependencies().entrySet().stream()
        .map(entry -> rethrowCall(() ->
            retrieveDependency(rootDir.getRootDir(), buildDir.getBuildDir(), NamePath.parse(entry.getKey()),
                entry.getValue().normalize(entry.getKey(), depErrors))
                ? Optional.<NamePath>empty()
                : Optional.of(NamePath.parse(entry.getKey()))))
        .flatMap(Optional::stream)
        .forEach(failedDep -> depErrors.fatal("Could not retrieve dependency: %s", failedDep));
  }

  private void copyFilesToBuildDir(ErrorCollector errors) throws IOException {
    Map<String, Optional<Path>> destinationPaths = copyScriptFilesToBuildDir().entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey, v->canonicalizePath(v.getValue())));
    //Files should exist, if error occurs its internal, hence we create root error collector
    addFileToPackageJsonConfig(buildDir.getBuildDir(), config.getScriptConfig(),
        destinationPaths, errors);

  }

  public static void addFileToPackageJsonConfig(Path rootDir, ScriptConfig scriptConfig, Map<String, Optional<Path>> filesByKey,
      ErrorCollector errors) {
    filesByKey.forEach((key, file) -> {
      if (file.isPresent()) {
        errors.checkFatal(Files.isRegularFile(file.get()), "Could not locate %s file: %s", key, file.get());
        String normalizedPath = rootDir.relativize(file.get()).normalize().toString();
        if (key.equals(MAIN_KEY)) {
          scriptConfig.setMainScript(normalizedPath);
        } else if (key.equals(GRAPHQL_KEY)) {
          scriptConfig.setGraphql(normalizedPath);
        }
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
    ScriptConfig scriptConfig = config.getScriptConfig();
    Map<String, Optional<Path>> destinationPaths = new HashMap<>();
    if (scriptConfig.getMainScript().isPresent()) {
      Path destinationPath = copyRelativeFile(rootDir.getRootDir().resolve(scriptConfig.getMainScript().get()), rootDir.getRootDir(),
          buildDir.getBuildDir());
      destinationPaths.put(MAIN_KEY,Optional.of(destinationPath));
    }
    if (scriptConfig.getGraphql().isPresent()) {
      Path destinationPath = copyRelativeFile(rootDir.getRootDir().resolve(scriptConfig.getGraphql().get()), rootDir.getRootDir(),
          buildDir.getBuildDir());
      destinationPaths.put(GRAPHQL_KEY,Optional.of(destinationPath));
    }
    return destinationPaths;
  }

  /**
   * Helper function to preprocess files.
   */
  private void preProcessFiles(PackageJson config, ErrorCollector errors) throws IOException {
    //Preprocessor will normalize files
    preprocessors.handle(
        PreprocessorsContext.builder()
            .rootDir(rootDir.getRootDir())
            .buildDir(buildDir.getBuildDir())
            .config(config)
            .errors(errors)
            .build());
  }

  private void writePackageConfig() throws IOException {
    config.toFile(buildDir.getBuildDir().resolve(PACKAGE_JSON), true);
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

  private boolean retrieveDependency(Path rootDir, Path buildDir, NamePath packagePath, Dependency dependency)
      throws IOException {
    Path targetPath = namepath2Path(buildDir, packagePath);
    Preconditions.checkArgument(FileUtil.isEmptyDirectory(targetPath),
        "Dependency [%s] conflicts with existing module structure in directory: [%s]", dependency,
        targetPath);

    // Determine the directory in the root that corresponds to the dependency's name
    String depName = dependency.getName();
    Path sourcePath = rootDir.resolve(depName);

    // Check if the source directory exists and is indeed a directory
    if (Files.isDirectory(sourcePath)) {
      // Copy the entire directory from source to target
      Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          Path targetDir = targetPath.resolve(sourcePath.relativize(dir));
          Files.createDirectories(targetDir);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.copy(file, targetPath.resolve(sourcePath.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }
      });
      return true;
    } else {
      // If the directory does not exist or is not a directory, proceed with the original retrieval logic
      return repository.retrieveDependency(targetPath, dependency);
    }
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

  @SneakyThrows
  public void postprocess(PackageJson sqrlConfig, Path rootDir, Path targetDir, PhysicalPlan plan, Optional<Path> mountDirectory,
      TestPlan testPlan, String[] profiles) {

    Map<String, Object> plans = new HashMap<>();
    // We'll write a single asset for each folder in the physical plan stage
    for (StagePlan stagePlan : plan.getStagePlans()) {
      Object planObj = writePlan(stagePlan.getStage().getName(), stagePlan.getPlan(), targetDir);
      plans.put(stagePlan.getStage().getName(), planObj);
    }

    if (testPlan != null) {
      Files.createDirectories(buildDir.getBuildDir().resolve(PLAN_DIR_NAME));
      Path path = buildDir.getBuildDir().resolve(PLAN_DIR_NAME).resolve("test.json");

      SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), testPlan);
      Map map = SqrlObjectMapper.INSTANCE.readValue(path.toFile(), Map.class);
      plans.put("test", map);
    }

    // Copy profiles
    for (String profile : profiles) {
      copyToDeploy(targetDir,
          rootDir.resolve(profile), plan, testPlan, sqrlConfig, mountDirectory, plans);
    }

    copyFolder(targetDir, DATA_DIR);
    copyFolder(targetDir, LIB_DIR);
  }

  @SneakyThrows
  private void copyFolder(Path targetDir, String folderName) {
    Path targetPath = targetDir.resolve("flink").resolve(folderName);
    Files.createDirectories(targetPath);
    Path sourcePath = buildDir.getBuildDir().resolve(folderName);
    if (Files.isDirectory(sourcePath)) {
      Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  @SneakyThrows
  private Object writePlan(String name, EnginePhysicalPlan plan, Path targetDir) {
    Files.createDirectories(buildDir.getBuildDir().resolve(PLAN_DIR_NAME));
    Path path = buildDir.getBuildDir().resolve(PLAN_DIR_NAME).resolve(name + ".json");

    SqrlObjectMapper.INSTANCE.enable(SerializationFeature.INDENT_OUTPUT);

    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);

    SqrlObjectMapper.INSTANCE.writer(prettyPrinter).writeValue(path.toFile(), plan);
    return SqrlObjectMapper.INSTANCE.readValue(path.toFile(), Map.class);
  }

  @SneakyThrows
  private void copyToDeploy(Path targetDir, Path profile, PhysicalPlan plan, TestPlan testPlan,
      PackageJson sqrlConfig, Optional<Path> mountDirectory, Map<String, Object> plans) {
    if (!Files.exists(targetDir)) {
      Files.createDirectories(targetDir);
    }

    Map<String, Object> templateConfig = new HashMap<>();
    templateConfig.put("config", sqrlConfig.toMap()); //Add SQRL config
    templateConfig.put("environment", System.getenv()); //Add environmental variables
    templateConfig.putAll(plans);
    mountDirectory.map(m->templateConfig.put("mountDir", m.toAbsolutePath().toString()));
    // Copy each file and directory from the profile path to the target directory
    if (!Files.isDirectory(profile)) {
      throw new RuntimeException("Could not find profile: " + profile);
    }
    Set<String> enabledEngines = new HashSet<>(sqrlConfig.getEnabledEngines());

    Set<String> possibleEngines = ServiceLoaderDiscovery.getAll(
        EngineFactory.class).stream()
        .map(e->e.getEngineName())
        .collect(Collectors.toSet());
    possibleEngines.add("test");

    try (Stream<Path> stream = Files.list(profile)) {
      List<Path> baseProfilePaths = stream.collect(Collectors.toList());
      for (Path sourcePath : baseProfilePaths) {
        //filter for engines
        String profileEngineName = sourcePath.getFileName().toString().split("\\.")[0];
        if (possibleEngines.contains(profileEngineName) && //Exclude any engines not selected
            !enabledEngines.contains(profileEngineName)) continue;
        if (sourcePath.getFileName().toString().equalsIgnoreCase("package.json")) continue;

        Path destinationPath = targetDir.resolve(profile.relativize(sourcePath)).toAbsolutePath();
        if (Files.isDirectory(destinationPath) || Files.isRegularFile(trimFtl(destinationPath))) continue; //skip existing to allow overloads

        copy(profileEngineName, profile, targetDir, sourcePath, templateConfig);
      }
    }
  }

  private Path trimFtl(Path destinationPath) {
    return destinationPath.getFileName().toString().endsWith(".ftl") ?
        destinationPath.getParent().resolve(destinationPath.getFileName().toString().substring(0,destinationPath.getFileName().toString().length()-4 ))
        : destinationPath;
  }

  @SneakyThrows
  private void copy(String profileEngineName, Path profile, Path targetDir, Path sourcePath,
      Map<String, Object> templateConfig) {
    try (Stream<Path> stream = Files.walk(sourcePath)) {
      List<Path> engineFiles = stream.collect(Collectors.toList());
      for (Path path : engineFiles) {
        Path destinationPath = targetDir.resolve(profile.relativize(path)).toAbsolutePath();
        if (Files.isDirectory(path)) {
          try {
            Files.createDirectories(destinationPath);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          if (path.toString().endsWith(".ftl")) {
            processTemplate(path, destinationPath, templateConfig);
          } else {
            Files.copy(path, destinationPath, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    }
  }

  public void processTemplate(Path path, Path destination, Map config) throws Exception {
    if (!path.toString().endsWith(".ftl")) {
      return;
    }

    // configure Freemarker
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_32);
    cfg.setDirectoryForTemplateLoading(path.getParent().toFile());
    cfg.setDefaultEncoding("UTF-8");
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    cfg.setNumberFormat("computer");

    cfg.setSharedVariable("jsonEncode", new JsonEncoderMethod());

    // extract the template filename
    String templateName = path.getFileName().toString();

    // load and process the template
    Template template = cfg.getTemplate(templateName);
    Writer out = new StringWriter();
    template.process(config, out);

    // remove .ftl extension
    String outputFileName = templateName.substring(0, templateName.length() - 4);

    // write
    Path outputPath = destination.getParent().resolve(outputFileName);
    Files.write(outputPath, out.toString().getBytes());
  }

  public class JsonEncoderMethod implements TemplateMethodModelEx {

    @Override
    public Object exec(List arguments) throws TemplateModelException {
      if (arguments.isEmpty()) {
        throw new TemplateModelException("JsonEncoderMethod expects one argument.");
      }
      try {
        Object obj = arguments.get(0);
        Object wrappedObject = ((DefaultMapAdapter) obj).getWrappedObject();
        return SqrlObjectMapper.INSTANCE.writeValueAsString(wrappedObject);
      } catch (Exception e) {
        throw new TemplateModelException("Error processing JSON encoding", e);
      }
    }
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

}
