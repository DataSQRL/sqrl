package com.datasqrl.config;

import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class SqrlConfigDebugger extends Debugger {

  @Inject
  public SqrlConfigDebugger(SqrlConfig config, @Named("debugFlag") boolean isDebug,
      ModuleLoader moduleLoader) {
    super(getConfig(config, isDebug), moduleLoader);
  }

  private static DebuggerConfig getConfig(SqrlConfig config, boolean isDebug) {
    CompilerConfiguration compilerConfig = CompilerConfiguration.fromRootConfig(config);

    DebuggerConfig debugger = DebuggerConfig.NONE;
    if (isDebug) debugger = compilerConfig.getDebugger();

    return debugger;
  }
}
