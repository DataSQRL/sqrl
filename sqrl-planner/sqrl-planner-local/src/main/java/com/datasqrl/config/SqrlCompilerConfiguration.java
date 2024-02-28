package com.datasqrl.config;

import com.google.inject.Inject;
import lombok.experimental.Delegate;

public class SqrlCompilerConfiguration extends CompilerConfiguration {

  @Delegate
  private final CompilerConfiguration compilerConfig;

  @Inject
  public SqrlCompilerConfiguration(SqrlConfig config) {
    this.compilerConfig = CompilerConfiguration.fromRootConfig(config);
  }
}
