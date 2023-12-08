package com.datasqrl.graphql.config;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.ApolloWSOptionsConverter;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ServletConfig {

  public ServletConfig() {
  }

  public ServletConfig(JsonObject json) {
    ServletConfigOptionsConverter.fromJson(json, this);
  }

  @Default
  String graphiQLEndpoint = "/graphiql/*";
  @Default
  String graphQLEndpoint = "/graphql";
  @Default
  boolean usePgPool = true;
  @Default
  public boolean useApolloWs = true;
  @Default
  public boolean allowRest = false;
  @Default
  public String graphQLWsEndpoint = "/graphql-ws";

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    ServletConfigOptionsConverter.toJson(this, json);
    return json;
  }
}
