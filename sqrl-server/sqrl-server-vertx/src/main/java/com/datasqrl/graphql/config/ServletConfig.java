package com.datasqrl.graphql.config;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ServletConfig {

  public ServletConfig() {}

  public ServletConfig(JsonObject json) {
    ServletConfigOptionsConverter.fromJson(json, this);
  }

  @Default String graphiQLEndpoint = "/graphiql*";
  @Default String graphQLEndpoint = "/graphql";
  @Default boolean usePgPool = true;
}
