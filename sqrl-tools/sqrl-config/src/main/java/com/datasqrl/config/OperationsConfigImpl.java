package com.datasqrl.config;

import com.datasqrl.config.PackageJson.OperationsConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
@Builder
@AllArgsConstructor
public class OperationsConfigImpl implements OperationsConfig {

  /** Whether to generate operations from the GraphQL schema */
  @Default boolean generate = true;

  /** This suffix it appended to all table names (before the uid) */
  @Default boolean addPrefix = true;

  /** The maximum depth of graph traversal when generating operations from schema */
  @Default int maxDepth = 3;

  public static OperationsConfigImpl from(SqrlConfig config) {
    var builder = builder();
    config.asBool("generate").getOptional().ifPresent(builder::generate);
    config.asBool("add-prefix").getOptional().ifPresent(builder::addPrefix);
    config.asInt("max-depth").getOptional().ifPresent(builder::maxDepth);
    return builder.build();
  }
}
