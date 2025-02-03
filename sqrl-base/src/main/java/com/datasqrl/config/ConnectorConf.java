package com.datasqrl.config;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Value;

public interface ConnectorConf {

  Map<String, Object> toMap();

  Map<String, Object> toMapWithSubstitution(Map<String, String> variables);

  default Map<String, Object> toMapWithSubstitution(Context context) {
    return toMapWithSubstitution(context.toVariables());
  }

  void validate(String key, Predicate<String> validator, String msg);


  @Value
  @Builder
  class Context {
    String tableName;

    public Map<String,String> toVariables() {
      Map<String,String> vars = new HashMap<>();
      if (tableName!=null) vars.put("tableName", tableName);
      return vars;
    }
  }

}
