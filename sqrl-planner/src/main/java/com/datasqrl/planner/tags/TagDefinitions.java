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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The tags defined under {@code compiler.tags}. Tag names and values are case-insensitive. When
 * {@code compiler.tags} is absent, tags are {@link #DISABLED} and doc strings are plain text.
 */
public final class TagDefinitions {

  public static final TagDefinitions DISABLED = new TagDefinitions(false, Map.of());

  /**
   * A tag in a doc string is {@code @name} or {@code @name=value}, preceded by whitespace or the
   * start of the text, so {@code user@example.com} is not a tag.
   */
  private static final Pattern TAG_PATTERN =
      Pattern.compile("(?<!\\S)@([A-Za-z0-9_-]+)(?:=([A-Za-z0-9_-]+))?");

  private final boolean enabled;
  private final Map<String, TagDefinition> definitions;

  private TagDefinitions(boolean enabled, Map<String, TagDefinition> definitions) {
    this.enabled = enabled;
    this.definitions = definitions;
  }

  public static TagDefinitions of(Collection<TagDefinition> definitions) {
    var byName = new LinkedHashMap<String, TagDefinition>();
    definitions.forEach(definition -> byName.put(definition.name(), definition));
    return new TagDefinitions(true, byName);
  }

  /**
   * Builds the tag definitions from the configuration. The package schema validates the syntax, so
   * this only checks what the schema cannot: duplicates that differ in case only, and the required
   * column types. Records all configuration errors without aborting.
   *
   * @param configs the configured tags by name, empty when tags are not configured
   * @param typeParser parses a required column type, throws if the type is invalid
   */
  public static TagDefinitions from(
      Optional<Map<String, TagConfig>> configs,
      Function<String, RelDataType> typeParser,
      ErrorCollector errors) {
    if (configs.isEmpty()) {
      return DISABLED;
    }
    errors = errors.abortOnFatal(false);
    var definitions = new LinkedHashMap<String, TagDefinition>();
    for (var entry : configs.get().entrySet()) {
      var name = normalize(entry.getKey());
      if (definitions.containsKey(name)) {
        errors.fatal("Duplicate tag [%s] in compiler.tags: tag names are case-insensitive", name);
        continue;
      }
      var values = new ArrayList<String>();
      for (var value : entry.getValue().getValues()) {
        if (values.contains(normalize(value))) {
          errors.fatal(
              "Duplicate value [%s] of tag [%s] in compiler.tags: values are case-insensitive",
              value, name);
        } else {
          values.add(normalize(value));
        }
      }
      var columns = new ArrayList<RequiredColumn>();
      for (var column : entry.getValue().getRequiredColumns().entrySet()) {
        try {
          columns.add(
              new RequiredColumn(
                  column.getKey(), column.getValue(), typeParser.apply(column.getValue())));
        } catch (Exception e) {
          errors.fatal(
              "Invalid type [%s] of required column [%s] of tag [%s] in compiler.tags: %s",
              column.getValue(), column.getKey(), name, e.getMessage());
        }
      }
      definitions.put(name, TagDefinition.of(name, values, columns));
    }
    return new TagDefinitions(true, definitions);
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Optional<TagDefinition> get(String name) {
    return Optional.ofNullable(definitions.get(normalize(name)));
  }

  /**
   * Resolves the tags written in a text, such as a doc string or column comment, against the
   * definitions. Returns {@link TableTags#EMPTY} when tags are disabled.
   *
   * @param text the text, may be null
   * @param onInvalid receives a message for each tag that is undefined or has an invalid value;
   *     such tags are not part of the result
   */
  public TableTags parse(String text, Consumer<String> onInvalid) {
    if (!enabled || text == null) {
      return TableTags.EMPTY;
    }
    var values = new ArrayList<TagValue>();
    var matcher = TAG_PATTERN.matcher(text);
    while (matcher.find()) {
      resolve(matcher.group(), matcher.group(1), matcher.group(2), onInvalid)
          .ifPresent(values::add);
    }
    return TableTags.of(values);
  }

  private Optional<TagValue> resolve(
      String tag, String name, String value, Consumer<String> onInvalid) {
    var definition = get(name);
    if (definition.isEmpty()) {
      onInvalid.accept("Tag [%s] is not defined in compiler.tags".formatted(tag));
      return Optional.empty();
    }
    var def = definition.get();
    if (value == null && !def.isBoolean()) {
      onInvalid.accept(
          "Tag [%s] requires a value: allowed values are %s".formatted(tag, def.values()));
      return Optional.empty();
    }
    var level = def.values().indexOf(value == null ? TagDefinition.TRUE : normalize(value));
    if (level < 0) {
      onInvalid.accept(
          "Tag [%s] has an invalid value: allowed values are %s".formatted(tag, def.values()));
      return Optional.empty();
    }
    return Optional.of(new TagValue(def, level));
  }

  /** Tag names and values are case-insensitive, so they are compared in lower case. */
  public static String normalize(String nameOrValue) {
    return nameOrValue.toLowerCase(Locale.ROOT);
  }
}
