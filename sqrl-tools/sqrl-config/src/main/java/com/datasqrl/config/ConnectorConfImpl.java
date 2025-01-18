package com.datasqrl.config;

import com.datasqrl.config.SqrlConfig.Value;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ConnectorConfImpl implements ConnectorConf {
  SqrlConfig sqrlConfig;

  @Override
  public Map<String, Object> toMap() {
    return sqrlConfig.toMap();
  }

  public Map<String, Object> toMapWithSubstitution(Map<String, String> variables) {
    return replaceVariablesInValues(toMap(), variables);
  }

  @Override
  public void validate(String key, Predicate<String> validator, String msg) {
    String value = sqrlConfig.asString(key).validate(validator, msg).get();
    Preconditions.checkArgument(value!=null, "Should not be null: %", key);
  }

  private Map<String, Object> replaceVariablesInValues(Map<String, Object> configMap,
      Map<String, String> variables) {
    if (configMap.isEmpty() || variables.isEmpty()) return configMap;

    Map<Pattern, String> variableMatcher = variables.entrySet().stream()
        .collect(Collectors.toMap(e -> Pattern.compile(getVariableRegex(e.getKey()), Pattern.CASE_INSENSITIVE),
            Map.Entry::getValue));

    Map<String, Object> resultMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof String) {
        String strValue = (String) value;
        for (Map.Entry<Pattern, String> varMatch : variableMatcher.entrySet()) {
          strValue = varMatch.getKey().matcher(strValue).replaceAll(varMatch.getValue());
        }
        //Make sure all variables have been replaced
        findSqrlVariable(strValue).ifPresent(s -> validate(entry.getKey(), x -> false,
            "Value contains invalid variable name: " + s));
        resultMap.put(entry.getKey(), strValue);
      } else {
        resultMap.put(entry.getKey(), value);
      }
    }

    return resultMap;
  }

  public static final String SQRL_VAR_PREFIX = "sqrl:";

  public static final Pattern SQRL_VARIABLE_FINDER = Pattern.compile("\\$\\{sqrl:(?<identifier>[\\w.-]+)\\}", Pattern.CASE_INSENSITIVE);

  private static String getVariableRegex(String variableName) {
    return Pattern.quote("${" + SQRL_VAR_PREFIX + variableName.toLowerCase() + "}");
  }

  private static Optional<String> findSqrlVariable(String value) {
    Matcher matcher = SQRL_VARIABLE_FINDER.matcher(value);
    if (matcher.find()) {
      return Optional.of(matcher.group("identifier"));
    } else {
      return Optional.empty();
    }
  }

}
