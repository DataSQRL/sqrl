package com.datasqrl.graphql.config;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class CorsHandlerOptions {

  public CorsHandlerOptions() {
  }

  public CorsHandlerOptions(JsonObject json) {
    CorsHandlerOptionsConverter.fromJson(json, this);
  }

  private String allowedOrigin;
  @Default
  private List<String> allowedOrigins;
  @Default
  private boolean allowCredentials = false;
  @Default
  private Integer maxAgeSeconds = -1;
  @Default
  private boolean allowPrivateNetwork = false;
  @Default
  private Set<String> allowedMethods = new LinkedHashSet<>();
  @Default
  private Set<String> allowedHeaders = new LinkedHashSet<>();
  @Default
  private Set<String> exposedHeaders = new LinkedHashSet<>();

  public JsonObject toJson() {
    var json = new JsonObject();
    CorsHandlerOptionsConverter.toJson(this, json);
    return json;
  }
}
