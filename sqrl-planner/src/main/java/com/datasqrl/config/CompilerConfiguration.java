/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.Constraints.Default;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@Getter
public class CompilerConfiguration {

  @Default
  String errorSink = "print.errors";

  @Default
  int maxApiArguments = 5;

  @Default
  String debugSink = "print";

  @Default
  ExplainConfig explain = new ExplainConfig();

  @Default
  List<String> debugTables = List.of();

  @Default
  boolean addArguments = true;

  public static final String COMPILER_KEY = "compiler";

  public static CompilerConfiguration fromRootConfig(@NonNull SqrlConfig rootConfig) {
    return fromConfig(rootConfig.getSubConfig(COMPILER_KEY));
  }

  public static CompilerConfiguration fromConfig(@NonNull SqrlConfig config) {
    return config.allAs(CompilerConfiguration.class).get();
  }


  public DebuggerConfig getDebugger() {
    NamePath sinkBasePath = NamePath.parse(debugSink);
    Set<Name> debugTables = null;
    if (this.debugTables !=null && !this.debugTables.isEmpty()) {
      debugTables = this.debugTables.stream().map(Name::system).collect(Collectors.toSet());
    }
    return DebuggerConfig.builder().enabled(true)
        .sinkBasePath(sinkBasePath)
        .debugTables(debugTables)
        .build();
  }

  public static ErrorLocation getDebuggerLocation() {
    return ErrorPrefix.CONFIG.resolve(COMPILER_KEY).resolve("debugSink");
  }

  @NoArgsConstructor
  @Getter
  public static class ExplainConfig {

    /**
     * Whether to produce a visual explanation
     */
    @Default
    boolean visual = true;
    /**
     * Whether to produce a textual explanation
     */
    @Default
    boolean text = true;
    /**
     * Whether to print out hints and other extended information in the pipeline explanation
     */
    @Default
    boolean extended = true;
    /**
     * This setting is primarily used for testing to ensure that the output of explain is deterministic
     */
    @Default
    boolean sorted = true; //TODO: set to false and overwrite in test case injector
  }



}
