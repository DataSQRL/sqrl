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
package com.datasqrl.config;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/** TODO: change value type in map to 'String' */
public interface ConnectorConf {

  Map<String, String> toMap();

  Map<String, String> toMapWithSubstitution(Map<String, String> variables);

  default Map<String, String> toMapWithSubstitution(Context context) {
    return toMapWithSubstitution(context.toVariables());
  }

  void validate(String key, Predicate<String> validator, String msg);

  @Value
  @Builder
  class Context {
    String tableName;
    String tableId;
    String filename;
    String format;
    @Singular Map<String, String> variables;

    public Map<String, String> toVariables() {
      Map<String, String> vars = new HashMap<>(variables);
      if (tableName != null) {
        vars.put("table-name", tableName);
      }
      if (tableId != null) {
        vars.put("table-id", tableId);
      }
      if (filename != null) {
        vars.put("filename", filename);
      }
      if (format != null) {
        vars.put("format", format);
      }
      return vars;
    }
  }
}
