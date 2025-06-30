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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ConnectorConfImpl implements ConnectorConf {
  SqrlConfig sqrlConfig;

  @Override
  public Map<String, String> toMap() {
    return (Map) sqrlConfig.toMap();
  }

  @Override
  public Map<String, String> toMapWithSubstitution(Map<String, String> variables) {
    return replaceVariablesInValues(sqrlConfig.toMap(), variables);
  }

  @Override
  public void validate(String key, Predicate<String> validator, String msg) {
    var value = sqrlConfig.asString(key).validate(validator, msg).get();
    Preconditions.checkArgument(value != null, "Should not be null: %", key);
  }

  private Map<String, String> replaceVariablesInValues(
      Map<String, Object> configMap, Map<String, String> variables) {
    Map<Pattern, String> variableMatcher =
        variables.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> Pattern.compile(getVariableRegex(e.getKey()), Pattern.CASE_INSENSITIVE),
                    Map.Entry::getValue));

    Map<String, String> resultMap = new TreeMap<>();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      var value = entry.getValue();
      if (value instanceof String strValue) {
        for (Map.Entry<Pattern, String> varMatch : variableMatcher.entrySet()) {
          strValue = varMatch.getKey().matcher(strValue).replaceAll(varMatch.getValue());
        }
        // Make sure all variables have been replaced
        findSqrlVariable(strValue)
            .ifPresent(
                s ->
                    validate(
                        entry.getKey(), x -> false, "Value contains invalid variable name: " + s));
        resultMap.put(entry.getKey(), strValue);
      } else if (value != null) {
        resultMap.put(entry.getKey(), String.valueOf(value));
      }
    }

    return resultMap;
  }

  public static final String SQRL_VAR_PREFIX = "sqrl:";

  public static final Pattern SQRL_VARIABLE_FINDER =
      Pattern.compile("\\$\\{sqrl:(?<identifier>[\\w.-]+)\\}", Pattern.CASE_INSENSITIVE);

  private static String getVariableRegex(String variableName) {
    return Pattern.quote("${" + SQRL_VAR_PREFIX + variableName.toLowerCase() + "}");
  }

  private static Optional<String> findSqrlVariable(String value) {
    var matcher = SQRL_VARIABLE_FINDER.matcher(value);
    if (matcher.find()) {
      return Optional.of(matcher.group("identifier"));
    } else {
      return Optional.empty();
    }
  }
}
