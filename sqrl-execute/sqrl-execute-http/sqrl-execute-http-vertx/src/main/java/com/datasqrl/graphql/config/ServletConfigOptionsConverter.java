package com.datasqrl.graphql.config;

import io.vertx.core.json.JsonObject;

public class ServletConfigOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ServletConfig obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "graphiQLEndpoint":
          if (member.getValue() instanceof String) {
            obj.setGraphiQLEndpoint(((String)member.getValue()));
          }
          break;
        case "graphQLEndpoint":
          if (member.getValue() instanceof String) {
            obj.setGraphQLEndpoint(((String)member.getValue()));
          }
          break;
        case "usePgPool":
          if (member.getValue() instanceof Boolean) {
            obj.setUsePgPool(((Boolean)member.getValue()).booleanValue());
          }
          break;
        case "useApolloWs":
          if (member.getValue() instanceof Boolean) {
            obj.setUseApolloWs(((Boolean)member.getValue()).booleanValue());
          }
          break;
        case "graphQLWsEndpoint":
          if (member.getValue() instanceof String) {
            obj.setGraphQLWsEndpoint(((String)member.getValue()));
          }
          break;
      }
    }
  }


  public static void toJson(ServletConfig obj, JsonObject json) {
    if (obj.getGraphiQLEndpoint() != null) {
      json.put("graphiQLEndpoint", obj.getGraphiQLEndpoint());
    }
    if (obj.getGraphQLEndpoint() != null) {
      json.put("graphQLEndpoint", obj.getGraphQLEndpoint());
    }
    json.put("usePgPool", obj.isUsePgPool());
    json.put("useApolloWs", obj.isUseApolloWs());
    if (obj.getGraphQLWsEndpoint() != null) {
      json.put("graphQLWsEndpoint", obj.getGraphQLWsEndpoint());
    }
  }
}
