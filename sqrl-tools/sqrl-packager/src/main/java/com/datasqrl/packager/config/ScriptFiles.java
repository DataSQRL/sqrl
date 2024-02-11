package com.datasqrl.packager.config;

import static com.datasqrl.packager.config.ScriptConfiguration.FILE_KEYS;
import static com.datasqrl.packager.config.ScriptConfiguration.fromScriptConfig;

import com.datasqrl.config.SqrlConfig;
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
    SqrlConfig config = fromScriptConfig(rootConfig);
    Map<String,Optional<String>> scriptFiles = Arrays.stream(FILE_KEYS)
        .collect(Collectors.toMap(Function.identity(),
            fileKey -> config.asString(fileKey).getOptional()));
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    this.scriptFiles = scriptFiles;
  }

  public Optional<String> get(String name) {
    return scriptFiles.get(name);
  }
}
