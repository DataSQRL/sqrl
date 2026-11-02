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
package com.datasqrl.planner.tags;

import com.datasqrl.config.PackageJson.TagConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.tags.TagDefinition.RequiredColumn;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The tag definitions configured under {@code compiler.tags}. When tags are not configured, {@link
 * #DISABLED} is used and tag processing is turned off.
 *
 * <p>Instances are compared by identity: there is a single instance per compilation.
 */
public final class TagDefinitions {

  public static final TagDefinitions DISABLED = new TagDefinitions(false, Map.of());

  static final Pattern NAME_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

  private final boolean enabled;
  private final Map<String, TagDefinition> definitions;

  private TagDefinitions(boolean enabled, Map<String, TagDefinition> definitions) {
    this.enabled = enabled;
    this.definitions = definitions;
  }

  /** Creates enabled tag definitions; mostly used in tests. */
  public static TagDefinitions of(Collection<TagDefinition> definitions) {
    var map = new LinkedHashMap<String, TagDefinition>();
    definitions.forEach(def -> map.put(def.name(), def));
    return new TagDefinitions(true, Map.copyOf(map));
  }

  /**
   * Builds the tag definitions from the configuration and reports configuration errors.
   *
   * @param configs the configured tags, empty when tags are not configured
   * @param typeParser parses a required-column type, throws if the type cannot be parsed
   * @param errors the error collector for configuration errors
   * @return the tag definitions, or {@link #DISABLED} if tags are not configured
   */
  public static TagDefinitions from(
      Optional<List<TagConfig>> configs,
      Function<String, RelDataType> typeParser,
      ErrorCollector errors) {
    if (configs.isEmpty()) {
      return DISABLED;
    }
    var result = new LinkedHashMap<String, TagDefinition>();
    for (var config : configs.get()) {
      var name = normalize(config.getName());
      if (name == null || !NAME_PATTERN.matcher(name).matches()) {
        errors.error(
            "Invalid tag name [%s] in compiler.tags: only letters, digits, underscores and dashes"
                + " are allowed",
            config.getName());
        continue;
      }
      if (result.containsKey(name)) {
        errors.error("Duplicate tag [%s] in compiler.tags", name);
        continue;
      }
      var values = new ArrayList<String>();
      for (var value : config.getValues()) {
        var normalized = normalize(value);
        if (normalized == null || !NAME_PATTERN.matcher(normalized).matches()) {
          errors.error(
              "Invalid value [%s] for tag [%s] in compiler.tags: only letters, digits, underscores"
                  + " and dashes are allowed",
              value, name);
        } else if (values.contains(normalized)) {
          errors.error("Duplicate value [%s] for tag [%s] in compiler.tags", normalized, name);
        } else {
          values.add(normalized);
        }
      }
      var columns = new ArrayList<RequiredColumn>();
      var columnNames = new HashSet<String>();
      for (var column : config.getRequiredColumns()) {
        if (!columnNames.add(column.getName().toLowerCase(Locale.ROOT))) {
          errors.error(
              "Duplicate required column [%s] for tag [%s] in compiler.tags",
              column.getName(), name);
          continue;
        }
        try {
          columns.add(
              new RequiredColumn(
                  column.getName(), column.getType(), typeParser.apply(column.getType())));
        } catch (Exception e) {
          errors.error(
              "Invalid type [%s] for required column [%s] of tag [%s] in compiler.tags: %s",
              column.getType(), column.getName(), name, e.getMessage());
        }
      }
      result.put(name, new TagDefinition(name, List.copyOf(values), List.copyOf(columns)));
    }
    return new TagDefinitions(true, Collections.unmodifiableMap(result));
  }

  public boolean isEnabled() {
    return enabled;
  }

  /** Looks up the definition of the given tag name, case-insensitive. */
  public Optional<TagDefinition> get(String name) {
    return Optional.ofNullable(definitions.get(normalize(name)));
  }

  public TagDefinition getOrThrow(String name) {
    return get(name).orElseThrow(() -> new IllegalArgumentException("Undefined tag: " + name));
  }

  public Collection<TagDefinition> getAll() {
    return definitions.values();
  }

  static String normalize(String s) {
    return s == null ? null : s.trim().toLowerCase(Locale.ROOT);
  }

  @Override
  public String toString() {
    return enabled ? "TagDefinitions" + definitions.keySet() : "TagDefinitions[disabled]";
  }
}
