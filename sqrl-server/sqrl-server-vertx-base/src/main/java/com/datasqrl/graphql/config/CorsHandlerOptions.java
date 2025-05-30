/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.graphql.config;

import io.vertx.core.json.JsonObject;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class CorsHandlerOptions {

  public CorsHandlerOptions() {}

  public CorsHandlerOptions(JsonObject json) {
    CorsHandlerOptionsConverter.fromJson(json, this);
  }

  private String allowedOrigin;
  @Default private List<String> allowedOrigins;
  @Default private boolean allowCredentials = false;
  @Default private Integer maxAgeSeconds = -1;
  @Default private boolean allowPrivateNetwork = false;
  @Default private Set<String> allowedMethods = new LinkedHashSet<>();
  @Default private Set<String> allowedHeaders = new LinkedHashSet<>();
  @Default private Set<String> exposedHeaders = new LinkedHashSet<>();

  public JsonObject toJson() {
    var json = new JsonObject();
    CorsHandlerOptionsConverter.toJson(this, json);
    return json;
  }
}
