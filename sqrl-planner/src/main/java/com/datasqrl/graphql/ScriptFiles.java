package com.datasqrl.graphql;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.google.inject.Inject;
import lombok.Getter;

public class ScriptFiles {

  @Getter private final ScriptConfig config;

  @Inject
  public ScriptFiles(PackageJson rootConfig) {
    this.config = rootConfig.getScriptConfig();
  }
}
