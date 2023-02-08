package com.datasqrl.packager;

import static com.datasqrl.packager.Packager.mapper;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class PackagerConfig {

  Path rootDir;
  Path mainScript;
  Path graphQLSchemaFile;
  @Builder.Default
  @NonNull
  List<Path> packageFiles = List.of();

  Repository repository;

  public Packager getPackager(ErrorCollector errors) throws IOException {
    checkRequiredArguments(errors);
    List<Path> packageFiles = getPackageFiles(errors);
    ObjectNode packageConfig = getPackageConfig(packageFiles);
    Repository repository = getRepository();
    GlobalPackageConfiguration config = mapper.convertValue(packageConfig,
        GlobalPackageConfiguration.class);
    return createPackager(packageFiles, packageConfig, repository, config, errors);
  }

  private void checkRequiredArguments(ErrorCollector errors) {
    errors.checkFatal(mainScript == null || Files.isRegularFile(mainScript),
        "Not a script file: %s", mainScript);
    errors.checkFatal(graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile),
        "Not a schema file: %s", graphQLSchemaFile);
  }

  private List<Path> getPackageFiles(ErrorCollector errors) {
    if (this.packageFiles.isEmpty()) {
      errors.checkFatal(mainScript != null,
          "Must provide either a main script or package file");

      //Try to find it in the directory of the main script
      if (mainScript != null) {
        Path packageFile = mainScript.getParent().resolve(Packager.PACKAGE_FILE_NAME);
        if (Files.isRegularFile(packageFile)) {
          return List.of(packageFile);
        }
      }
    }
    return this.packageFiles;
  }

  private ObjectNode getPackageConfig(List<Path> packageFiles) throws IOException {
    ObjectNode packageConfig;
    GlobalPackageConfiguration config;
    if (packageFiles.isEmpty()) {
      Path rootDir = getRootDir();
      config = GlobalPackageConfiguration.builder()
          .manifest(
              buildRelativizeManifest(rootDir, mainScript, Optional.ofNullable(graphQLSchemaFile)))
          .build();
      packageConfig = mapper.valueToTree(config);
    } else {
      Path rootDir = getRootDirFromPackageFiles();
      JsonNode basePackage = mapper.readValue(packageFiles.get(0).toFile(), JsonNode.class);
      packageFiles.stream()
          .skip(1)
          .forEach(path -> updatePackageFile(mapper, basePackage, path));
      packageConfig = (ObjectNode) basePackage;
      config = mapper.convertValue(packageConfig, GlobalPackageConfiguration.class);
      updateManifest(rootDir, config);
      JsonNode mappedManifest = mapper.valueToTree(config.getManifest());
      packageConfig.set(ManifestConfiguration.MANIFEST, mappedManifest);
    }
    return packageConfig;
  }

  private Path getRootDir() {
    Path rootDir = this.rootDir;
    if (rootDir == null) {
      rootDir = mainScript.getParent();
    }
    return rootDir;
  }

  private void updatePackageFile(ObjectMapper mapper, JsonNode basePackage, Path path) {
    try {
      mapper.readerForUpdating(basePackage).readValue(path.toFile());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Path getRootDirFromPackageFiles() {
    Path rootDir = this.rootDir;
    if (rootDir == null) {
      rootDir = packageFiles.get(0).getParent();
    }
    return rootDir;
  }

  private void updateManifest(Path rootDir, GlobalPackageConfiguration config) {
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
    config.setManifest(buildRelativizeManifest(rootDir, main, Optional.ofNullable(graphql)));
  }

  private Repository getRepository() {
    if (this.repository == null) {
      return new RemoteRepositoryImplementation();
    }
    return this.repository;
  }

  private Packager createPackager(List<Path> packageFiles, ObjectNode packageConfig,
      Repository repository, GlobalPackageConfiguration config, ErrorCollector errors) {
    return new Packager(repository,
        packageFiles.isEmpty() ? getRootDir() : getRootDirFromPackageFiles(),
        packageConfig, config, errors);
  }

  public static ManifestConfiguration buildRelativizeManifest(Path rootDir, Path mainScript,
      Optional<Path> graphQLSchemaFile) {
    Preconditions.checkArgument(mainScript != null || !Files.isRegularFile(mainScript),
        "Must configure a main script");
    ManifestConfiguration.ManifestConfigurationBuilder builder = ManifestConfiguration.builder();
    builder.main(rootDir.relativize(mainScript).normalize().toString());
    graphQLSchemaFile.ifPresent(gql -> {
      Preconditions.checkArgument(Files.isRegularFile(gql));
      builder.graphql(rootDir.relativize(gql).normalize().toString());
    });

    return builder.build();
  }
}
