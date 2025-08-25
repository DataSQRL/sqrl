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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class CorsHandlerOptions {

  private String allowedOrigin;
  private List<String> allowedOrigins;
  private boolean allowCredentials = false;
  private Integer maxAgeSeconds = -1;
  private boolean allowPrivateNetwork = false;
  private Set<String> allowedMethods = new LinkedHashSet<>();
  private Set<String> allowedHeaders = new LinkedHashSet<>();
  private Set<String> exposedHeaders = new LinkedHashSet<>();

  public CorsHandlerOptions(JsonObject json) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "allowedOrigin":
          if (member.getValue() instanceof String) {
            this.allowedOrigin = (String) member.getValue();
          }
          break;
        case "allowedOrigins":
          if (member.getValue() instanceof Iterable c) {
            this.allowedOrigins = toList(c);
          }
          break;
        case "allowCredentials":
          if (member.getValue() instanceof Boolean) {
            this.allowCredentials = (Boolean) member.getValue();
          }
          break;
        case "maxAgeSeconds":
          if (member.getValue() instanceof Integer) {
            this.maxAgeSeconds = (Integer) member.getValue();
          }
          break;
        case "allowPrivateNetwork":
          if (member.getValue() instanceof Boolean) {
            this.allowPrivateNetwork = (Boolean) member.getValue();
          }
          break;
        case "allowedMethods":
          if (member.getValue() instanceof Iterable c) {
            this.allowedMethods = toSet(c);
          }
          break;
        case "allowedHeaders":
          if (member.getValue() instanceof Iterable c) {
            this.allowedHeaders = toSet(c);
          }
          break;
        case "exposedHeaders":
          if (member.getValue() instanceof Iterable c) {
            this.exposedHeaders = toSet(c);
          }
          break;
      }
    }
  }

  private static List<String> toList(Iterable<?> c) {
    return StreamSupport.stream(c.spliterator(), false).map(String.class::cast).toList();
  }

  private static Set<String> toSet(Iterable<?> c) {
    return StreamSupport.stream(c.spliterator(), false)
        .map(String.class::cast)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }
}
