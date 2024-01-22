package com.datasqrl.packager;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class PackagerConfig {

  Path rootDir;
  Path mainScript;
  Path graphQLSchemaFile;
  SqrlConfig config;
  String[] profiles;

  Repository repository;

  public Packager getPackager(ErrorCollector errors) throws IOException {
    checkRequiredArguments(errors);
    Repository repository = getRepository(errors);
    updateScriptConfig(errors);
    return new Packager(repository, rootDir, config, profiles, errors);
  }

  private void checkRequiredArguments(ErrorCollector errors) {
    errors.checkFatal(rootDir!=null && Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    errors.checkFatal(mainScript == null || Files.isRegularFile(mainScript), "Could not find script file: %s", mainScript);
    errors.checkFatal(graphQLSchemaFile == null || Files.isRegularFile(graphQLSchemaFile), "Could not find API file: %s", graphQLSchemaFile);
  }

  private void updateScriptConfig(ErrorCollector errors) {
    SqrlConfig scriptConfig = ScriptConfiguration.fromRootConfig(config);
    setScriptFiles(rootDir, scriptConfig, ImmutableMap.of(ScriptConfiguration.MAIN_KEY, Optional.ofNullable(mainScript),
        ScriptConfiguration.GRAPHQL_KEY, Optional.ofNullable(graphQLSchemaFile)), errors);
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

  public static void setScriptFiles(Path rootDir, SqrlConfig config, Map<String, Optional<Path>> filesByKey,
      ErrorCollector errors) {
    filesByKey.forEach((key, file) -> {
      if (file.isPresent()) {
        errors.checkFatal(Files.isRegularFile(file.get()), "Could not locate %s file: %s", key, file);
        config.setProperty(key,rootDir.relativize(file.get()).normalize().toString());
      }
    });
  }

}
