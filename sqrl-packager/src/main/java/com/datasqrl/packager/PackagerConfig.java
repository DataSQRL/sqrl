package com.datasqrl.packager;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.packager.util.Serializer;
import com.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.datasqrl.packager.Packager.mapper;

@Builder
@Value
public class PackagerConfig {

  Path rootDir;
  Path mainScript;
  Path graphQLSchemaFile;
  List<Path> packageFiles;

  Repository repository;

  public Packager getPackager(ErrorCollector errors) throws IOException {
    checkRequiredArguments(errors);
    ObjectNode packageConfig = getPackageConfig(packageFiles);
    Repository repository = getRepository(errors);
    GlobalPackageConfiguration config = mapper.convertValue(packageConfig,
        GlobalPackageConfiguration.class);
    return new Packager(repository, rootDir, packageConfig, config, errors);
  }

  private void checkRequiredArguments(ErrorCollector errors) {
    errors.checkFatal(packageFiles!=null && !packageFiles.isEmpty(), "Need to configure a package manifest");
    packageFiles.forEach( pkg -> errors.checkFatal(Files.isRegularFile(pkg),
            "Could not find package file: %s", pkg));
    errors.checkFatal(rootDir!=null && Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    errors.checkFatal(mainScript == null || Files.isRegularFile(mainScript), "Could not find script file: %s", mainScript);
    errors.checkFatal(graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile), "Could not find API file: %s", graphQLSchemaFile);
  }

  private ObjectNode getPackageConfig(List<Path> packageFiles) throws IOException {
    ObjectNode packageConfig;
    GlobalPackageConfiguration config;
    JsonNode basePackage = Serializer.combineFiles(packageFiles);
    packageConfig = (ObjectNode) basePackage;
    config = mapper.convertValue(packageConfig, GlobalPackageConfiguration.class);
    updateManifest(rootDir, config);
    JsonNode mappedManifest = mapper.valueToTree(config.getManifest());
    packageConfig.set(ManifestConfiguration.PROPERTY, mappedManifest);
    return packageConfig;
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

  private Repository getRepository(ErrorCollector errors) {
    if (this.repository == null) {
      LocalRepositoryImplementation localRepo = LocalRepositoryImplementation.of(errors);
      //TODO: read remote repository URLs from configuration?
      RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation();
      remoteRepo.setCacheRepository(localRepo);
      return new CompositeRepositoryImpl(List.of(localRepo, remoteRepo));
    }
    return this.repository;
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
