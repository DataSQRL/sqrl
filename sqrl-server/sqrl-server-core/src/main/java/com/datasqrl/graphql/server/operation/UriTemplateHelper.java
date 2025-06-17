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
