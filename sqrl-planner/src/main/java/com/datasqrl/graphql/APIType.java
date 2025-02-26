package com.datasqrl.graphql;

import com.google.common.base.Strings;
import java.util.Optional;

public enum APIType {
  GraphQL;

  public static Optional<APIType> get(String apiType) {
    if (Strings.isNullOrEmpty(apiType)) return Optional.empty();
    apiType = apiType.trim();
    for (APIType a : values()) {
      if (a.name().equalsIgnoreCase(apiType)) return Optional.of(a);
    }
    return Optional.empty();
  }
}
