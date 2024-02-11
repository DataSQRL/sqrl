package com.datasqrl;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FileUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;

public class MainScriptImpl implements MainScript {

  public static final String SCRIPT_KEY = "script";
  public static final String MAIN_KEY = "main";
  public static final String GRAPHQL_KEY = "graphql";
  public static final String[] FILE_KEYS = {MAIN_KEY, GRAPHQL_KEY};

  private final SqrlConfig config;
  private final ResourceResolver resourceResolver;

  @Inject
  public MainScriptImpl(SqrlConfig config, ResourceResolver resourceResolver) {
    this.config = config;
    this.resourceResolver = resourceResolver;
  }

  public String getContent() {
    URI mainScript = getMainScript(config).flatMap(resourceResolver::resolveFile)
        .orElseThrow(() -> new RuntimeException("Could not find main sqrl script file"));
    return FileUtil.readFile(mainScript);
  }

  private Optional<NamePath> getMainScript(SqrlConfig config) {
    Map<String, Optional<String>> scriptFiles = getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    return scriptFiles.get(MAIN_KEY).map(NamePath::of);
  }
  public static Map<String,Optional<String>> getFiles(@NonNull SqrlConfig rootConfig) {
    SqrlConfig config = fromScriptConfig(rootConfig);
    return Arrays.stream(FILE_KEYS).collect(Collectors.toMap(Function.identity(),
        fileKey -> config.asString(fileKey).getOptional()));
  }

  public static SqrlConfig fromScriptConfig(@NonNull SqrlConfig rootConfig) {
    return rootConfig.getSubConfig(SCRIPT_KEY);
  }


  @Override
  public Optional<Path> getPath() {
    return Optional.empty(); //todo fix me
  }
}
