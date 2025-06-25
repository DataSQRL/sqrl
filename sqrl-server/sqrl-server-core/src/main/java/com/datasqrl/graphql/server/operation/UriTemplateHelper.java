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
package com.datasqrl.graphql.server.operation;

import java.util.Map;

public class UriTemplateHelper {

  public static String getQueryUriTemplate(FunctionDefinition function, String queryName) {
    StringBuilder template = new StringBuilder();
    template.append("queries/").append(queryName);

    FunctionDefinition.Parameters params = function.getParameters();
    if (params != null && params.getProperties() != null && !params.getProperties().isEmpty()) {
      template.append("{?");
      boolean first = true;
      for (Map.Entry<String, FunctionDefinition.Argument> entry :
          params.getProperties().entrySet()) {
        String paramName = entry.getKey();
        if (!first) {
          template.append(",");
        }
        template.append(paramName);
        first = false;
      }
      template.append("}");
    }
    return template.toString();
  }

  public static String getMutationUriTemplate(String mutationName) {
    return "mutations/" + mutationName;
  }
}
