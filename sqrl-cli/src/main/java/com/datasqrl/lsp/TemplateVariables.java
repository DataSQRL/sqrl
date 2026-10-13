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
package com.datasqrl.lsp;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionItemKind;

/**
 * The variables of the templates in scripts, e.g. {@code {{variant}}}, which the CLI fills in from
 * the {@code script.config} of the package files: those of the project for its own scripts, and
 * those of an included package, with the overrides of the include, for the scripts of the package.
 * Nested values are named with dots, e.g. {@code {{kafka.topic}}}.
 */
@Slf4j
final class TemplateVariables {

  /** A variable in a template, e.g. {@code {{ variant }}} or {@code {{#flag}}} */
  private static final Pattern VARIABLE = Pattern.compile("\\{\\{[#^/]?\\s*([\\w.]+)\\s*}}");

  /** A tag of a template, with the kind of tag, e.g. {@code #} for a section */
  private static final Pattern TAG = Pattern.compile("\\{\\{([#^/!>&{]?)\\s*([\\w.]+)\\s*}?}}");

  /**
   * A variable in a template.
   *
   * @param name its name
   * @param line the 0-based line of the tag
   * @param start where the tag starts in the line
   * @param end where the tag ends in the line
   */
  record Tag(String name, int line, int start, int end) {}

  private TemplateVariables() {}

  /**
   * The variables of a script by name, with their values as text.
   *
   * @param script the script
   * @param packageFiles the package files of the project of the script
   */
  static Map<String, String> of(Path script, List<Path> packageFiles) {
    if (packageFiles.isEmpty()) {
      return Map.of();
    }
    try {
      var config =
          ConfigLoaderUtils.loadUnresolvedTestConfig(ErrorCollector.root(), packageFiles)
              .getScriptConfig();
      var projectDir = packageFiles.get(0).getParent();
      for (var include : config.getIncludeConfigs()) {
        var includeFile = projectDir.resolve(include.getPackage()).normalize();
        if (script.startsWith(includeFile.getParent())) {
          var values =
              new LinkedHashMap<>(
                  ConfigLoaderUtils.loadResolvedConfigFromFile(ErrorCollector.root(), includeFile)
                      .getScriptConfig()
                      .getConfig());
          values.putAll(include.getConfig());
          return flatten(values);
        }
      }
      return flatten(config.getConfig());
    } catch (RuntimeException e) {
      // e.g. while a package file is being edited
      log.debug("Could not read the template variables of {}", script, e);
      return Map.of();
    }
  }

  /**
   * The variables of the templates in a script that are not defined, which are replaced with
   * nothing. Variables in sections are left out, since they can be fields of the value of the
   * section.
   */
  static List<Tag> unknown(String text, Map<String, String> variables) {
    var result = new ArrayList<Tag>();
    var lines = text.split("\n", -1);
    var sections = 0;
    for (var line = 0; line < lines.length; line++) {
      var tags = TAG.matcher(lines[line]);
      while (tags.find()) {
        var name = tags.group(2);
        switch (tags.group(1)) {
          case "#", "^" -> sections++;
          case "/" -> sections = Math.max(0, sections - 1);
          case "", "&", "{" -> {
            if (sections == 0 && !isDefined(name, variables)) {
              result.add(new Tag(name, line, tags.start(), tags.end()));
            }
          }
          default -> {}
        }
      }
    }
    return result;
  }

  /**
   * Whether a variable has a value, or is a map of values, e.g. {@code kafka} of {@code
   * kafka.topic}.
   */
  private static boolean isDefined(String name, Map<String, String> variables) {
    return variables.containsKey(name)
        || variables.keySet().stream().anyMatch(key -> key.startsWith(name + "."));
  }

  /** The variable whose name is at an offset, if any. */
  static Optional<String> nameAt(String text, int offset) {
    var lineStart = text.lastIndexOf('\n', Math.max(0, offset - 1)) + 1;
    var lineEnd = text.indexOf('\n', offset);
    var line = text.substring(lineStart, lineEnd < 0 ? text.length() : lineEnd);
    var matcher = VARIABLE.matcher(line);
    while (matcher.find()) {
      if (matcher.start() <= offset - lineStart && offset - lineStart <= matcher.end()) {
        return Optional.of(matcher.group(1));
      }
    }
    return Optional.empty();
  }

  /** The variables to complete a template with, with their values. */
  static List<CompletionItem> complete(Map<String, String> variables) {
    return variables.entrySet().stream()
        .map(
            variable -> {
              var item = new CompletionItem(variable.getKey());
              item.setKind(CompletionItemKind.Variable);
              item.setDetail(variable.getValue());
              return item;
            })
        .toList();
  }

  /** The values of nested maps by their names with dots. */
  private static Map<String, String> flatten(Map<String, Object> values) {
    var result = new LinkedHashMap<String, String>();
    flatten("", values, result);
    return result;
  }

  private static void flatten(String prefix, Map<?, ?> values, Map<String, String> result) {
    values.forEach(
        (key, value) -> {
          var name = prefix + key;
          if (value instanceof Map<?, ?> nested) {
            flatten(name + ".", nested, result);
          } else {
            result.put(name, String.valueOf(value));
          }
        });
  }
}
