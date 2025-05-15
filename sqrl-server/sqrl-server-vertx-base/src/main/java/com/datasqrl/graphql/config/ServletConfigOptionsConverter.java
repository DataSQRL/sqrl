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

public class ServletConfigOptionsConverter {

  public static void fromJson(
      Iterable<java.util.Map.Entry<String, Object>> json, ServletConfig obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "graphiQLEndpoint":
          if (member.getValue() instanceof String) {
            obj.setGraphiQLEndpoint(((String) member.getValue()));
          }
          break;
        case "graphQLEndpoint":
          if (member.getValue() instanceof String) {
            obj.setGraphQLEndpoint(((String) member.getValue()));
          }
          break;
        case "usePgPool":
          if (member.getValue() instanceof Boolean) {
            obj.setUsePgPool(((Boolean) member.getValue()));
          }
          break;
      }
    }
  }
}
