package com.datasqrl.config;

import com.google.inject.Inject;

import lombok.experimental.Delegate;

public class SqrlCompilerConfiguration implements PackageJson.CompilerConfig {

  @Delegate
  private final PackageJson.CompilerConfig compilerConfig;

  @Inject
  public SqrlCompilerConfiguration(PackageJson config) {
    this.compilerConfig = config.getCompilerConfig();
  }
}
