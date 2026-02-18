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
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;

@Slf4j
public class FlinkConfigValidator {

  private static final Set<String> DEPLOYMENT_KEYS =
      Set.of("taskmanager-size", "jobmanager-size", "taskmanager-count", "secrets", "schedule");

  private static final List<String> FLINK_OPTIONS_PACKAGES =
      List.of(
          "org.apache.flink.configuration",
          "org.apache.flink.streaming.api.environment",
          "org.apache.flink.table.api.config");

  private static final Set<String> KNOWN_KEYS = buildKnownKeys();
  private static final Set<String> KNOWN_PREFIXES = buildKnownPrefixes();

  private static Set<String> buildKnownKeys() {
    var keys = new HashSet<String>();
    for (var pkg : FLINK_OPTIONS_PACKAGES) {
      for (var clazz : findOptionsClasses(pkg)) {
        extractConfigOptionKeys(clazz, keys);
      }
    }
    return keys;
  }

  private static Set<Class<?>> findOptionsClasses(String packageName) {
    var classes = new HashSet<Class<?>>();
    var path = packageName.replace('.', '/');
    try {
      var urls = Thread.currentThread().getContextClassLoader().getResources(path);
      while (urls.hasMoreElements()) {
        var url = urls.nextElement();
        if (!"jar".equals(url.getProtocol())) {
          continue;
        }
        var connection = (JarURLConnection) url.openConnection();
        try (var jarFile = new JarFile(connection.getJarFile().getName())) {
          var entries = jarFile.entries();
          while (entries.hasMoreElements()) {
            var entry = entries.nextElement();
            var name = entry.getName();
            if (name.startsWith(path + "/")
                && name.endsWith("Options.class")
                && !name.contains("$")) {
              var className = name.replace('/', '.').replace(".class", "");
              try {
                classes.add(Class.forName(className));
              } catch (ClassNotFoundException | NoClassDefFoundError e) {
                log.debug("Cannot load class: {}", className);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      log.debug("Cannot scan package {}: {}", packageName, e.getMessage());
    }
    return classes;
  }

  private static void extractConfigOptionKeys(Class<?> clazz, Set<String> keys) {
    for (Field field : clazz.getDeclaredFields()) {
      if (Modifier.isPublic(field.getModifiers())
          && Modifier.isStatic(field.getModifiers())
          && ConfigOption.class.isAssignableFrom(field.getType())) {
        try {
          var option = (ConfigOption<?>) field.get(null);
          keys.add(option.key());
          for (var fallback : option.fallbackKeys()) {
            keys.add(fallback.getKey());
          }
        } catch (IllegalAccessException e) {
          log.debug("Cannot access field {} in {}", field.getName(), clazz.getName());
        }
      }
    }
  }

  private static Set<String> buildKnownPrefixes() {
    var prefixes = new HashSet<String>();
    for (var key : KNOWN_KEYS) {
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
    for (var key : config.keySet()) {
      if (DEPLOYMENT_KEYS.contains(key)) {
        errors.warn(
            "Key '%s' belongs in 'engines.flink.deployment', not 'engines.flink.config'", key);
      } else if (!KNOWN_KEYS.contains(key)) {
        warnUnknownKey(key, errors);
      }
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
}
