package com.datasqrl.graphql;


import com.datasqrl.config.SqrlConfig;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScriptFiles {

  private final Map<String, Optional<String>> scriptFiles;

  @Inject
  public ScriptFiles(SqrlConfig rootConfig) {
    SqrlConfig config = ScriptConfiguration.fromScriptConfig(rootConfig);
    Map<String,Optional<String>> scriptFiles = Arrays.stream(ScriptConfiguration.FILE_KEYS)
        .collect(Collectors.toMap(Function.identity(),
            fileKey -> config.asString(fileKey).getOptional()));
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    this.scriptFiles = scriptFiles;
  }

  public Optional<String> get(String name) {
    return scriptFiles.get(name);
  }
}
