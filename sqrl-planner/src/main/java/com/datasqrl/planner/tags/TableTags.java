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

import com.datasqrl.planner.tags.InvalidTag.Reason;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * The tag set of a table. It references the {@link TagDefinitions} and maps each normalized tag
 * name to the offset of its value in the tag's configured values ({@link
 * TagDefinition#BOOLEAN_OFFSET} for boolean tags), so combining tag sets is a maximum over
 * integers.
 *
 * <p>Tags written in doc strings that don't resolve to a defined tag and value are kept as {@link
 * #getInvalidTags() invalid tags} so validation can report them.
 *
 * <p>Instances are immutable.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableTags {

  public static final TableTags EMPTY = new TableTags(TagDefinitions.DISABLED, Map.of(), List.of());

  @NonNull @EqualsAndHashCode.Exclude private final TagDefinitions definitions;

  /** Normalized tag name to the offset into the tag's values. */
  @NonNull private final Map<String, Integer> offsets;

  /** Tags from doc strings that don't resolve to a defined tag and value. */
  @NonNull private final List<InvalidTag> invalidTags;

  public static Builder builder(TagDefinitions definitions) {
    return new Builder(definitions);
  }

  public boolean isEmpty() {
    return offsets.isEmpty() && invalidTags.isEmpty();
  }

  public boolean hasInvalidTags() {
    return !invalidTags.isEmpty();
  }

  public OptionalInt get(String tag) {
    var offset = offsets.get(TagDefinitions.normalize(tag));
    return offset == null ? OptionalInt.empty() : OptionalInt.of(offset);
  }

  public boolean contains(String tag) {
    return offsets.containsKey(TagDefinitions.normalize(tag));
  }

  public Set<String> names() {
    return Collections.unmodifiableSet(offsets.keySet());
  }

  /** Returns the value name of the given tag; the tag must be contained in this set. */
  public String valueOf(String tag) {
    return definitions.getOrThrow(tag).valueAt(get(tag).orElseThrow());
  }

  /** The union of both tag sets with the maximum value per tag. Invalid tags are kept. */
  public TableTags merge(TableTags other) {
    if (other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }
    checkSameDefinitions(other);
    var merged = new HashMap<>(offsets);
    other.offsets.forEach((tag, offset) -> merged.merge(tag, offset, Math::max));
    var invalid = new ArrayList<>(invalidTags);
    invalid.addAll(other.invalidTags);
    return new TableTags(definitions, Map.copyOf(merged), List.copyOf(invalid));
  }

  /**
   * Declassifies the given tags: each tag gets its value from {@code declared}, or is removed if
   * {@code declared} doesn't contain it.
   */
  public TableTags declassify(Set<String> tags, TableTags declared) {
    if (tags.isEmpty() || offsets.isEmpty()) {
      return this;
    }
    if (!declared.isEmpty()) {
      checkSameDefinitions(declared);
    }
    var result = new HashMap<>(offsets);
    for (var tag : tags) {
      var name = TagDefinitions.normalize(tag);
      var declaredOffset = declared.offsets.get(name);
      if (declaredOffset == null) {
        result.remove(name);
      } else if (result.containsKey(name)) {
        result.put(name, declaredOffset);
      }
    }
    return new TableTags(definitions, Map.copyOf(result), invalidTags);
  }

  /** Returns this tag set without its invalid tags. */
  public TableTags withoutInvalidTags() {
    if (invalidTags.isEmpty()) {
      return this;
    }
    if (offsets.isEmpty()) {
      return EMPTY;
    }
    return new TableTags(definitions, offsets, List.of());
  }

  /** Maps each tag name to its value name ({@code true} for boolean tags), sorted by name. */
  public SortedMap<String, String> toValueMap() {
    var result = new TreeMap<String, String>();
    offsets.forEach((tag, offset) -> result.put(tag, definitions.getOrThrow(tag).valueAt(offset)));
    return Collections.unmodifiableSortedMap(result);
  }

  /** Renders the tags sorted by name, e.g. {@code classification=secret, pii=raw}. */
  @Override
  public String toString() {
    return toValueMap().entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(", "));
  }

  private void checkSameDefinitions(TableTags other) {
    Preconditions.checkArgument(
        definitions == other.definitions, "Cannot combine tags of different tag definitions");
  }

  /** Builds the tag set of a doc string. */
  public static final class Builder {

    private final TagDefinitions definitions;
    private final Map<String, Integer> offsets = new HashMap<>();
    private final List<InvalidTag> invalidTags = new ArrayList<>();

    private Builder(TagDefinitions definitions) {
      this.definitions = definitions;
    }

    /**
     * Adds a tag as written in a doc string. Names and values are normalized to lower case, a
     * missing value means {@code true}, and the maximum value is kept for duplicate tags. Tags that
     * don't resolve to a defined tag and value are recorded as invalid tags. Does nothing if tags
     * are disabled.
     *
     * @param name the tag name
     * @param value the tag value, or null if none was given
     */
    public Builder add(String name, String value) {
      if (!definitions.isEnabled()) {
        return this;
      }
      var tag = TagDefinitions.normalize(name);
      var normalizedValue = TagDefinitions.normalize(value);
      var text = "@" + tag + (normalizedValue == null ? "" : "=" + normalizedValue);
      var definition = definitions.get(tag);
      if (definition.isEmpty()) {
        invalidTags.add(new InvalidTag(text, tag, Reason.UNDEFINED_TAG));
        return this;
      }
      var def = definition.get();
      if (!def.isBoolean() && normalizedValue == null) {
        invalidTags.add(new InvalidTag(text, tag, Reason.MISSING_VALUE));
        return this;
      }
      var offset = def.offsetOf(normalizedValue == null ? TagDefinition.TRUE : normalizedValue);
      if (offset.isEmpty()) {
        invalidTags.add(
            new InvalidTag(
                text, tag, def.isBoolean() ? Reason.NON_TRUE_BOOLEAN : Reason.INVALID_VALUE));
        return this;
      }
      offsets.merge(tag, offset.getAsInt(), Math::max);
      return this;
    }

    /** Adds a tag by its value offset; used to build tag sets programmatically. */
    public Builder addOffset(String name, int offset) {
      var def = definitions.getOrThrow(name);
      def.valueAt(offset); // validates the offset
      offsets.merge(def.name(), offset, Math::max);
      return this;
    }

    public TableTags build() {
      if (offsets.isEmpty() && invalidTags.isEmpty()) {
        return EMPTY;
      }
      return new TableTags(definitions, Map.copyOf(offsets), List.copyOf(invalidTags));
    }
  }
}
