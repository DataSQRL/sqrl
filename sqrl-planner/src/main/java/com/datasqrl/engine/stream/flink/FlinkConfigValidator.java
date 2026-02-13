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
package com.datasqrl.engine.stream.flink;

import com.datasqrl.error.ErrorCollector;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;

@Slf4j
public class FlinkConfigValidator {

  private static final Set<String> DEPLOYMENT_KEYS =
      Set.of("taskmanager-size", "jobmanager-size", "taskmanager-count", "secrets", "schedule");

  private static final Pattern DURATION_PATTERN =
      Pattern.compile(
          "\\d+\\s*(ns|nano|nanos|nanosecond|nanoseconds"
              + "|us|micro|micros|microsecond|microseconds"
              + "|ms|milli|millis|millisecond|milliseconds"
              + "|s|sec|secs|second|seconds"
              + "|min|minute|minutes"
              + "|h|hour|hours"
              + "|d|day|days)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern MEMORY_PATTERN =
      Pattern.compile(
          "\\d+\\s*(b|bytes|k|kb|kibibytes"
              + "|m|mb|mebibytes"
              + "|g|gb|gibibytes"
              + "|t|tb|tebibytes)",
          Pattern.CASE_INSENSITIVE);

  private static final List<String> FLINK_OPTIONS_CLASSES =
      List.of(
          "org.apache.flink.configuration.CoreOptions",
          "org.apache.flink.configuration.ExecutionOptions",
          "org.apache.flink.configuration.CheckpointingOptions",
          "org.apache.flink.configuration.StateBackendOptions",
          "org.apache.flink.configuration.RestOptions",
          "org.apache.flink.configuration.TaskManagerOptions",
          "org.apache.flink.configuration.JobManagerOptions",
          "org.apache.flink.configuration.PipelineOptions",
          "org.apache.flink.configuration.SecurityOptions",
          "org.apache.flink.configuration.HighAvailabilityOptions",
          "org.apache.flink.configuration.DeploymentOptions",
          "org.apache.flink.configuration.MetricOptions",
          "org.apache.flink.configuration.NettyShuffleEnvironmentOptions",
          "org.apache.flink.configuration.HeartbeatManagerOptions",
          "org.apache.flink.configuration.AkkaOptions",
          "org.apache.flink.configuration.BlobServerOptions",
          "org.apache.flink.configuration.ClusterOptions",
          "org.apache.flink.configuration.ResourceManagerOptions",
          "org.apache.flink.configuration.WebOptions",
          "org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions",
          "org.apache.flink.table.api.config.ExecutionConfigOptions",
          "org.apache.flink.table.api.config.OptimizerConfigOptions",
          "org.apache.flink.table.api.config.TableConfigOptions");

  private static final Map<String, ConfigOption<?>> KNOWN_OPTIONS = buildKnownOptions();
  private static final Set<String> KNOWN_PREFIXES = buildKnownPrefixes();

  private static Map<String, ConfigOption<?>> buildKnownOptions() {
    var options = new HashMap<String, ConfigOption<?>>();
    for (var className : FLINK_OPTIONS_CLASSES) {
      try {
        var clazz = Class.forName(className);
        for (Field field : clazz.getDeclaredFields()) {
          if (Modifier.isPublic(field.getModifiers())
              && Modifier.isStatic(field.getModifiers())
              && ConfigOption.class.isAssignableFrom(field.getType())) {
            try {
              var option = (ConfigOption<?>) field.get(null);
              options.put(option.key(), option);
              for (var fallback : option.fallbackKeys()) {
                options.put(fallback.getKey(), option);
              }
            } catch (IllegalAccessException e) {
              log.debug("Cannot access field {} in {}", field.getName(), className);
            }
          }
        }
      } catch (ClassNotFoundException e) {
        log.debug("Flink options class not on classpath: {}", className);
      }
    }
    return options;
  }

  private static Set<String> buildKnownPrefixes() {
    var prefixes = new HashSet<String>();
    for (var key : KNOWN_OPTIONS.keySet()) {
      var dot = key.indexOf('.');
      if (dot > 0) {
        prefixes.add(key.substring(0, dot));
      }
    }
    return prefixes;
  }

  public static void validate(Map<String, Object> config, ErrorCollector errors) {
    if (config == null || config.isEmpty()) {
      return;
    }
    for (var entry : config.entrySet()) {
      var key = entry.getKey();
      var value = entry.getValue();

      if (DEPLOYMENT_KEYS.contains(key)) {
        errors.warn(
            "Key '%s' belongs in 'engines.flink.deployment', not 'engines.flink.config'", key);
        continue;
      }

      var option = KNOWN_OPTIONS.get(key);
      if (option == null) {
        warnUnknownKey(key, errors);
        continue;
      }

      validateValueType(key, value, option, errors);
    }
  }

  private static void warnUnknownKey(String key, ErrorCollector errors) {
    var dot = key.indexOf('.');
    if (dot > 0) {
      var prefix = key.substring(0, dot);
      if (KNOWN_PREFIXES.contains(prefix)) {
        errors.warn(
            "Unrecognized Flink configuration key '%s' (prefix '%s.' is valid). Check for typos.",
            key, prefix);
        return;
      }
    }
    errors.warn("Unrecognized Flink configuration key '%s'. Check for typos.", key);
  }

  private static void validateValueType(
      String key, Object value, ConfigOption<?> option, ErrorCollector errors) {
    var expectedType = getOptionType(option);
    if (expectedType == null || String.class.isAssignableFrom(expectedType)) {
      return;
    }
    if (expectedType.isEnum()) {
      return;
    }

    var strValue = String.valueOf(value);

    if (Boolean.class.isAssignableFrom(expectedType)
        || boolean.class.isAssignableFrom(expectedType)) {
      if (!"true".equalsIgnoreCase(strValue) && !"false".equalsIgnoreCase(strValue)) {
        errors.warn(
            "Invalid value '%s' for Flink config key '%s'. Expected type: Boolean", strValue, key);
      }
    } else if (Integer.class.isAssignableFrom(expectedType)
        || int.class.isAssignableFrom(expectedType)) {
      try {
        Integer.parseInt(strValue);
      } catch (NumberFormatException e) {
        errors.warn(
            "Invalid value '%s' for Flink config key '%s'. Expected type: Integer", strValue, key);
      }
    } else if (Long.class.isAssignableFrom(expectedType)
        || long.class.isAssignableFrom(expectedType)) {
      try {
        Long.parseLong(strValue);
      } catch (NumberFormatException e) {
        errors.warn(
            "Invalid value '%s' for Flink config key '%s'. Expected type: Long", strValue, key);
      }
    } else if (isDuration(expectedType)) {
      if (!DURATION_PATTERN.matcher(strValue).matches()) {
        errors.warn(
            "Invalid value '%s' for Flink config key '%s'. Expected type: Duration"
                + " (e.g. '60s', '5min', '1h')",
            strValue, key);
      }
    } else if (isMemorySize(expectedType)) {
      if (!MEMORY_PATTERN.matcher(strValue).matches()) {
        errors.warn(
            "Invalid value '%s' for Flink config key '%s'. Expected type: MemorySize"
                + " (e.g. '256mb', '1gb')",
            strValue, key);
      }
    }
  }

  private static Class<?> getOptionType(ConfigOption<?> option) {
    try {
      var field = ConfigOption.class.getDeclaredField("clazz");
      field.setAccessible(true);
      return (Class<?>) field.get(option);
    } catch (ReflectiveOperationException e) {
      log.debug("Cannot read clazz from ConfigOption for key '{}'", option.key());
      return null;
    }
  }

  private static boolean isDuration(Class<?> clazz) {
    return clazz.getName().equals("java.time.Duration");
  }

  private static boolean isMemorySize(Class<?> clazz) {
    return clazz.getName().equals("org.apache.flink.configuration.MemorySize");
  }
}
