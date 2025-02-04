package com.datasqrl.config;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * TODO: change value type in map to 'String'
 */
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
    String origTableName;
    @Singular
    Map<String,String> variables;

    public Map<String,String> toVariables() {
      Map<String,String> vars = new HashMap<>(variables);
      if (tableName!=null) vars.put("table-name", tableName);
      if (origTableName!=null) vars.put("original-table-name", origTableName);
      return vars;
    }
  }

}
