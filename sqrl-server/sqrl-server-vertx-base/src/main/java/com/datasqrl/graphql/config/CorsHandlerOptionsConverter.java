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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CorsHandlerOptionsConverter {

  public static void fromJson(JsonObject json, CorsHandlerOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "allowedOrigin":
          if (member.getValue() instanceof String) {
            obj.setAllowedOrigin((String) member.getValue());
          }
          break;
        case "allowedOrigins":
          if (member.getValue() instanceof Iterable c) {
            obj.setAllowedOrigins(toList(c));
          }
          break;
        case "allowCredentials":
          if (member.getValue() instanceof Boolean) {
            obj.setAllowCredentials((Boolean) member.getValue());
          }
          break;
        case "maxAgeSeconds":
          if (member.getValue() instanceof Integer) {
            obj.setMaxAgeSeconds((Integer) member.getValue());
          }
          break;
        case "allowPrivateNetwork":
          if (member.getValue() instanceof Boolean) {
            obj.setAllowPrivateNetwork((Boolean) member.getValue());
          }
          break;
        case "allowedMethods":
          if (member.getValue() instanceof Iterable c) {
            obj.setAllowedMethods(toSet(c));
          }
          break;
        case "allowedHeaders":
          if (member.getValue() instanceof Iterable c) {
            obj.setAllowedHeaders(toSet(c));
          }
          break;
        case "exposedHeaders":
          if (member.getValue() instanceof Iterable c) {
            obj.setExposedHeaders(toSet(c));
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

  public static void toJson(CorsHandlerOptions obj, JsonObject json) {

    if (obj.getAllowedOrigin() != null) {
      json.put("allowedOrigin", obj.getAllowedOrigin());
    }
    if (obj.getAllowedOrigins() != null) {
      json.put("allowedOrigins", new ArrayList<>(obj.getAllowedOrigins()));
    }
    json.put("allowCredentials", obj.isAllowCredentials());
    json.put("maxAgeSeconds", obj.getMaxAgeSeconds());
    json.put("allowPrivateNetwork", obj.isAllowPrivateNetwork());
    if (obj.getAllowedMethods() != null) {
      json.put("allowedMethods", new LinkedHashSet<>(obj.getAllowedMethods()));
    }
    if (obj.getAllowedHeaders() != null) {
      json.put("allowedHeaders", new LinkedHashSet<>(obj.getAllowedHeaders()));
    }
    if (obj.getExposedHeaders() != null) {
      json.put("exposedHeaders", new LinkedHashSet<>(obj.getExposedHeaders()));
    }
  }
}
