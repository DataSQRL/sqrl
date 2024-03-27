/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.config.Constraints.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

@NoArgsConstructor
@Getter
public class CompilerConfiguration {


  @Default
  ExplainConfig explain = new ExplainConfig();

  @Default
  boolean addArguments = true;

  public static final String COMPILER_KEY = "compiler";

  public static CompilerConfiguration fromRootConfig(@NonNull SqrlConfig rootConfig) {
    return fromConfig(rootConfig.getSubConfig(COMPILER_KEY));
  }

  public static CompilerConfiguration fromConfig(@NonNull SqrlConfig config) {
    return config.allAs(CompilerConfiguration.class).get();
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
