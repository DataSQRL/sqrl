package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
    JsonObject json = new JsonObject();
    CorsHandlerOptionsConverter.toJson(this, json);
    return json;
  }
}
