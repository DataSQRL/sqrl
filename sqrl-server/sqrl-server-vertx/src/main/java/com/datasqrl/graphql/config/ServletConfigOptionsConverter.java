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
            obj.setUsePgPool(((Boolean) member.getValue()).booleanValue());
          }
          break;
      }
    }
  }
}
