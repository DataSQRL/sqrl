package com.datasqrl.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
@Builder
@AllArgsConstructor
public class ExplainConfigImpl implements PackageJson.ExplainConfig {

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
   * This setting is primarily used for testing to ensure that the output of explain is
   * deterministic
   */
  @Default
  boolean sorted = true; //TODO: set to false and overwrite in test case injector

  public ExplainConfigImpl(SqrlConfig sqrlConfig) {
    this(
        sqrlConfig.asBool("visual").getOptional().orElse(true),
        sqrlConfig.asBool("text").getOptional().orElse(true),
        sqrlConfig.asBool("extended").getOptional().orElse(true),
        sqrlConfig.asBool("sorted").getOptional().orElse(true));
  }
}
