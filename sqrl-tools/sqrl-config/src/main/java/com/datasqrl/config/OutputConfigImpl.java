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
public class OutputConfigImpl implements PackageJson.OutputConfig {

  /**
   * Whether to append a unique identifier to the generate tables in the engines
   */
  @Default
  boolean addUid = true;

  /**
   * This suffix it appended to all table names (before the uid)
   */
  @Default
  String tableSuffix = "";

  public static OutputConfigImpl from(SqrlConfig config) {
    OutputConfigImplBuilder builder = builder();
    config.asBool("add-uid").getOptional().ifPresent(builder::addUid);
    config.asString("table-suffix").getOptional().ifPresent(builder::tableSuffix);
    return builder.build();
  }

}
