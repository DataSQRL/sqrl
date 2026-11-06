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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

/** The tags of a table, with one value per tag. Instances are immutable. */
@EqualsAndHashCode
public final class TableTags {

  public static final TableTags EMPTY = new TableTags(new TreeMap<>());

  /** The tag values by tag name, sorted by name */
  private final SortedMap<String, TagValue> tags;

  private TableTags(SortedMap<String, TagValue> tags) {
    this.tags = Collections.unmodifiableSortedMap(tags);
  }

  /** The tag set of the given values, keeping the highest value of each tag. */
  public static TableTags of(Collection<TagValue> values) {
    var tags = new TreeMap<String, TagValue>();
    values.forEach(value -> tags.merge(value.name(), value, TableTags::max));
    return new TableTags(tags);
  }

  public boolean isEmpty() {
    return tags.isEmpty();
  }

  public Optional<TagValue> get(String name) {
    return Optional.ofNullable(tags.get(name));
  }

  /** The tag values, sorted by tag name. */
  public Collection<TagValue> values() {
    return tags.values();
  }

  /** The union of both tag sets, with the highest value per tag. */
  public TableTags merge(TableTags other) {
    var merged = new TreeMap<>(tags);
    other.tags.forEach((name, value) -> merged.merge(name, value, TableTags::max));
    return new TableTags(merged);
  }

  /** This tag set without the given tags. */
  public TableTags without(Set<String> names) {
    var remaining = new TreeMap<>(tags);
    remaining.keySet().removeAll(names);
    return new TableTags(remaining);
  }

  /** Maps each tag name to its value ({@code true} for boolean tags), sorted by name. */
  public SortedMap<String, String> toValueMap() {
    return tags.values().stream()
        .collect(
            Collectors.toMap(
                TagValue::name, TagValue::value, (a, b) -> a, TreeMap<String, String>::new));
  }

  /** Renders the tags as written in doc strings, e.g. {@code @classification=secret @gdpr}. */
  @Override
  public String toString() {
    return tags.values().stream().map(TagValue::toString).collect(Collectors.joining(" "));
  }

  private static TagValue max(TagValue a, TagValue b) {
    return a.isLowerThan(b) ? b : a;
  }
}
