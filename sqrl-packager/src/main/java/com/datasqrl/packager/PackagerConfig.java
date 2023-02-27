package com.datasqrl.packager;

import static com.datasqrl.packager.Packager.mapper;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.Deserializer;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.spi.ScriptConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;

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
    errors.checkFatal(packageFiles!=null && !packageFiles.isEmpty(), "Need to configure a package configuration");
    packageFiles.forEach( pkg -> errors.checkFatal(Files.isRegularFile(pkg),
            "Could not find package file: %s", pkg));
    errors.checkFatal(rootDir!=null && Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    errors.checkFatal(mainScript == null || Files.isRegularFile(mainScript), "Could not find script file: %s", mainScript);
    errors.checkFatal(graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile), "Could not find API file: %s", graphQLSchemaFile);
  }

  private ObjectNode getPackageConfig(List<Path> packageFiles) throws IOException {
    ObjectNode packageConfig;
    GlobalPackageConfiguration config;
    JsonNode basePackage = new Deserializer().combineJsonFiles(packageFiles);
    packageConfig = (ObjectNode) basePackage;
    config = mapper.convertValue(packageConfig, GlobalPackageConfiguration.class);
    updateScriptConfig(rootDir, config);
    JsonNode mappedScript = mapper.valueToTree(config.getScript());
    packageConfig.set(ScriptConfiguration.PROPERTY, mappedScript);
    return packageConfig;
  }

  private void updateScriptConfig(Path rootDir, GlobalPackageConfiguration config) {
    Path main = mainScript, graphql = graphQLSchemaFile;
    ScriptConfiguration script;
    if (config.getScript() != null) {
      script = config.getScript();
      if (main == null) {
        main = rootDir.resolve(script.getMain());
      }
      if (graphql == null && script.getOptGraphQL().isPresent()) {
        graphql = rootDir.resolve(script.getOptGraphQL().get());
      }
    }
    //Update script config
    config.setScript(buildRelativizeScriptConfig(rootDir, main, Optional.ofNullable(graphql)));
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

  public static ScriptConfiguration buildRelativizeScriptConfig(Path rootDir, Path mainScript,
      Optional<Path> graphQLSchemaFile) {
    Preconditions.checkArgument(mainScript != null || !Files.isRegularFile(mainScript),
        "Must configure a main script");
    ScriptConfiguration.ScriptConfigurationBuilder builder = ScriptConfiguration.builder();
    builder.main(rootDir.relativize(mainScript).normalize().toString());
    graphQLSchemaFile.ifPresent(gql -> {
      Preconditions.checkArgument(Files.isRegularFile(gql));
      builder.graphql(rootDir.relativize(gql).normalize().toString());
    });
    return builder.build();
  }
}
