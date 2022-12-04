package com.datasqrl.packager;

import com.datasqrl.loaders.DynamicExporter;
import com.datasqrl.loaders.DynamicLoader;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Value
public class Packager {

  public static final String BUILD_DIR_NAME = "build";
  public static final String GRAPHQL_SCHEMA_FILE_NAME = "schema.graphqls";
  public static final String PACKAGE_FILE_NAME = "package.json";
  public static final Set<String> EXCLUDED_DIRS = Set.of(BUILD_DIR_NAME, "deploy");

  Path rootDir;
  JsonNode packageConfig;
  GlobalPackageConfiguration config;

  private Packager(@NonNull Path rootDir, @NonNull JsonNode packageConfig,
      @NonNull GlobalPackageConfiguration config) {
    Preconditions.checkArgument(Files.isDirectory(rootDir));
    this.rootDir = rootDir;
    this.packageConfig = packageConfig;
    this.config = config;
  }

  public void inferDependencies() {
    LinkedHashMap<String, Dependency> dependencies = config.getDependencies();
    //TODO: add dependencies from imports; if the namepath prefix doesn't map onto directory or sqrl file it's an external dependency
    JsonNode mappedDepends = getMapper().valueToTree(dependencies);
    ((ObjectNode) packageConfig).set(GlobalPackageConfiguration.DEPENDENCIES_NAME, mappedDepends);
  }

  public Path populateBuildDir() {
    try {
      Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
      try {
        Files.deleteIfExists(buildDir);
      } catch (DirectoryNotEmptyException e) {
        throw new IllegalStateException(String.format(
            "Build directory [%s] already exists and is non-empty. Check and empty directory before running command again",
            buildDir));
      }
      Files.createDirectories(buildDir);
      Preconditions.checkArgument(
          config.getManifest() != null && !Strings.isNullOrEmpty(config.getManifest().getMain()));
      Path originalMainFile = rootDir.resolve(config.getManifest().getMain());
      Path mainFile = copyRelativeFile(originalMainFile, rootDir, buildDir);
      Optional<Path> originalGraphQLSchemaFile = config.getManifest().getOptGraphQL()
          .map(file -> rootDir.resolve(file));
      Path graphQLSchemaFile = null;
      if (originalGraphQLSchemaFile.isPresent()) {
        graphQLSchemaFile = copyFile(originalGraphQLSchemaFile.get(), buildDir,
            Path.of(GRAPHQL_SCHEMA_FILE_NAME));
      }
      ManifestConfiguration manifest = Config.getManifest(buildDir, mainFile, graphQLSchemaFile);
      JsonNode mappedManifest = getMapper().valueToTree(manifest);
      ((ObjectNode) packageConfig).set(ManifestConfiguration.PROPERTY, mappedManifest);

      //Update dependencies and write out
      Path packageFile = buildDir.resolve(PACKAGE_FILE_NAME);
      getMapper().writeValue(packageFile.toFile(), packageConfig);
      //Add external dependencies
      //TODO: implement
      Preconditions.checkArgument(config.getDependencies().isEmpty());
      //Recursively copy all files that can be handled by loaders
      DynamicLoader loader = new DynamicLoader();
      DynamicExporter exporter = new DynamicExporter();
      Predicate<Path> copyFilePredicate = path -> loader.usesFile(path) || exporter.usesFile(path);
      CopyFiles cpFiles = new CopyFiles(rootDir, buildDir, copyFilePredicate,
          EXCLUDED_DIRS.stream().map(dir -> rootDir.resolve(dir)).collect(Collectors.toList()));
      Files.walkFileTree(rootDir, cpFiles);
      return packageFile;
    } catch (IOException e) {
      throw new IllegalStateException("Could not read or write files on local file-system", e);
    }
  }

  public void cleanUp() {
    try {
      Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
      FileUtils.deleteDirectory(buildDir.toFile());
    } catch (IOException e) {
      throw new IllegalStateException("Could not read or write files on local file-system", e);
    }
  }

  @Value
  private class CopyFiles implements FileVisitor<Path> {

    Path srcDir;
    Path targetDir;
    Predicate<Path> copyFile;
    Collection<Path> excludedDirs;

    private boolean isExcludedDir(Path dir) throws IOException {
      for (Path p : excludedDirs) {
        if (dir.equals(p)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      if (isExcludedDir(dir)) {
        return FileVisitResult.SKIP_SUBTREE;
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if (copyFile.test(file)) {
        copyRelativeFile(file, srcDir, targetDir);
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      //TODO: collect error
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      return FileVisitResult.CONTINUE;
    }
  }

  public static Path copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
    return copyFile(srcFile, destDir, srcDir.relativize(srcFile));
  }

  public static Path copyFile(Path srcFile, Path destDir, Path relativeDestPath)
      throws IOException {
    Preconditions.checkArgument(Files.isRegularFile(srcFile));
    Path targetPath = destDir.resolve(relativeDestPath);
    Files.createDirectories(targetPath.getParent());
    Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }


  private static ObjectMapper getMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper;
  }

  @Builder
  @Value
  public static class Config {

    Path rootDir;
    Path mainScript;
    Path graphQLSchemaFile;
    @Builder.Default
    @NonNull
    List<Path> packageFiles = List.of();

    public Packager getPackager() throws IOException {
      Preconditions.checkArgument(mainScript == null || Files.isRegularFile(mainScript));
      Preconditions.checkArgument(
          graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile));
      List<Path> packageFiles = this.packageFiles;
      if (packageFiles.isEmpty()) {
        //Try to find it in the directory of the main script
        if (mainScript != null) {
          Path packageFile = mainScript.getParent().resolve(PACKAGE_FILE_NAME);
          if (Files.isRegularFile(packageFile)) {
            packageFiles = List.of(packageFile);
          }
        }
      }
      ObjectMapper mapper = getMapper();
      Path rootDir = this.rootDir;
      JsonNode packageConfig;
      GlobalPackageConfiguration config;
      if (packageFiles.isEmpty()) {
        Preconditions.checkArgument(mainScript != null,
            "Must provide either a main script or package file");
        if (rootDir == null) {
          rootDir = mainScript.getParent();
        }
        config = GlobalPackageConfiguration.builder()
            .manifest(getManifest(rootDir, mainScript, graphQLSchemaFile)).build();
        packageConfig = mapper.valueToTree(config);
      } else {
        if (rootDir == null) {
          rootDir = packageFiles.get(0).getParent();
        }
        JsonNode basePackage = mapper.readValue(packageFiles.get(0).toFile(), JsonNode.class);
        for (int i = 1; i < packageFiles.size(); i++) {
          basePackage = mapper.readerForUpdating(basePackage)
              .readValue(packageFiles.get(i).toFile());
        }
        packageConfig = basePackage;
        config = mapper.convertValue(packageConfig, GlobalPackageConfiguration.class);
        Path main = mainScript, graphql = graphQLSchemaFile;
        ManifestConfiguration manifest;
        if (config.getManifest() != null) {
          manifest = config.getManifest();
          if (main == null) {
            main = rootDir.resolve(manifest.getMain());
          }
          if (graphql == null && manifest.getOptGraphQL().isPresent()) {
            graphql = rootDir.resolve(manifest.getOptGraphQL().get());
          }
        }
        //Update manifest
        config.setManifest(getManifest(rootDir, main, graphql));
        JsonNode mappedManifest = mapper.valueToTree(config.getManifest());
        ((ObjectNode) packageConfig).set(ManifestConfiguration.PROPERTY, mappedManifest);
      }
      return new Packager(rootDir, packageConfig, config);
    }

    public static ManifestConfiguration getManifest(Path rootDir, Path mainScript,
        Path graphQLSchemaFile) {
      Preconditions.checkArgument(mainScript != null || !Files.isRegularFile(mainScript),
          "Must configure a main script");
      ManifestConfiguration.ManifestConfigurationBuilder builder = ManifestConfiguration.builder();
      builder.main(rootDir.relativize(mainScript).normalize().toString());
      if (graphQLSchemaFile != null) {
        Preconditions.checkArgument(Files.isRegularFile(graphQLSchemaFile));
        builder.graphql(rootDir.relativize(graphQLSchemaFile).normalize().toString());
      }
      return builder.build();
    }


  }

}
