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
   * Whether to print the SQL for the tables and queries as planned
   */
  @Default
  boolean sql = false;
  /**
   * Whether to print the logical plan for the tables and queries as a relational tree with hints
   */
  @Default
  boolean logical = true;
  /**
   * Whether to print the physical plan for the tables and queries
   */
  @Default
  boolean physical = false;
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
        sqrlConfig.asBool("sql").getOptional().orElse(false),
        sqrlConfig.asBool("logical").getOptional().orElse(true),
        sqrlConfig.asBool("physical").getOptional().orElse(false),
        sqrlConfig.asBool("sorted").getOptional().orElse(true));
  }
}
