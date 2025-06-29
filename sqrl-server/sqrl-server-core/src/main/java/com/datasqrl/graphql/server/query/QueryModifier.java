package com.datasqrl.graphql.server.query;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface QueryModifier {

  @JsonIgnore
  boolean isPreprocessor();

  @JsonIgnore
  default boolean isPostprocessor() {
    return !isPreprocessor();
  }
}
