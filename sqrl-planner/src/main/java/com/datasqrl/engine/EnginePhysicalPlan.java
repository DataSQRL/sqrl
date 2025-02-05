/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 * A jackson serializable object
 */
public interface EnginePhysicalPlan {

  @JsonIgnore
  default List<DeploymentArtifact> getDeploymentArtifacts() {
    return List.of();
  }

  @Value
  class DeploymentArtifact {
    String fileSuffix;
    Object content;

    public static String toSqlString(Stream<String> statements) {
      return statements.collect(Collectors.joining(";\n"));
    }

    public static String toSqlString(Collection<String> statements) {
      return toSqlString(statements.stream());
    }

  }

}
