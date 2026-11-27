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
package com.datasqrl.planner.contract;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.contract.DataContractFormat.Entries;
import com.datasqrl.planner.contract.DataContractFormat.Entry;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/**
 * A data contract file that sits next to a script, and the tables of the compilation that its
 * entries were applied to.
 */
public final class DataContractFile {

  /** A table that an entry was applied to. */
  public record AppliedTable(Path definingFile, String tableName) {}

  @Getter private final Path path;
  @Getter private final DataContractFormat format;

  /** Reports about this file, without aborting */
  private final ErrorCollector errors;

  /** Where the entries of the file start, for errors about the entries as a whole */
  private final FileLocation entriesLocation;

  private final boolean declaresTags;

  /** The entries by their name in lower case, in the order of the file */
  private final Map<String, Entry> entries = new LinkedHashMap<>();

  /** The table that each entry was applied to, by the name of the entry in lower case */
  private final Map<String, AppliedTable> appliedTables = new LinkedHashMap<>();

  /**
   * @param entries what the format read from the file
   * @param errors reports about the file, without aborting
   */
  DataContractFile(Path path, DataContractFormat format, Entries entries, ErrorCollector errors) {
    this.path = path;
    this.format = format;
    this.errors = errors;
    this.entriesLocation = entries.location();
    this.declaresTags = entries.declaresTags();
    entries.entries().forEach(entry -> this.entries.put(key(entry.name()), entry));
  }

  /** The entry of the table with the given name, ignoring case. */
  public Optional<Entry> getEntry(String tableName) {
    return Optional.ofNullable(entries.get(key(tableName)));
  }

  /**
   * Records that the entry describes the table, unless it already describes another one.
   *
   * @return the table that the entry describes
   */
  AppliedTable markApplied(Entry entry, AppliedTable table) {
    var applied = appliedTables.putIfAbsent(key(entry.name()), table);
    return applied == null ? table : applied;
  }

  /**
   * The names of the tables that entries of this file were applied to, in the order the tables were
   * planned.
   */
  public List<String> getAppliedTableNames() {
    return appliedTables.values().stream().map(AppliedTable::tableName).toList();
  }

  /** The entries that were not applied to a table, in file order. */
  public List<Entry> getUnappliedEntries() {
    return entries.entrySet().stream()
        .filter(entry -> !appliedTables.containsKey(entry.getKey()))
        .map(Map.Entry::getValue)
        .toList();
  }

  /** The names of the entries that were not applied to a table, in file order. */
  public List<String> getUnappliedEntryNames() {
    return getUnappliedEntries().stream().map(Entry::name).toList();
  }

  /** Whether an entry declares tags or a classification, for the table or for a column. */
  public boolean declaresTags() {
    return declaresTags;
  }

  /** The error collector that reports at the given location of this file. */
  public ErrorCollector errorsAt(FileLocation location) {
    return errors.atFile(location);
  }

  /** The error collector that reports about the entries of this file as a whole. */
  public ErrorCollector errorsAtEntries() {
    return errorsAt(entriesLocation);
  }

  private static String key(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
